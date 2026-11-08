// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
use std::net::IpAddr;
use std::sync::Arc;

use lore_base::error::AddressNotFound;
use lore_base::lore_spawn;
use lore_base::runtime::LORE_CONTEXT;
use lore_base::types::Address;
use lore_base::types::Hash;
use lore_proto::BranchPushRequest;
use lore_proto::BranchPushResponse;
use lore_revision::branch;
use lore_revision::branch::BranchError;
use lore_revision::branch::LATEST;
use lore_revision::branch::PROTECT;
use lore_revision::branch::load_latest;
use lore_revision::branch::metadata;
use lore_revision::branch::push;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::notification::NotificationSender;
use lore_revision::repository;
use lore_revision::repository::RepositoryContext;
use lore_revision::state;
use lore_revision::state::State;
use lore_storage::StoreError;
use lore_storage::StoreMatch;
use lore_storage::StoreMatchResult;
use lore_telemetry::InstrumentProvider;
use lore_telemetry::tracing::fields::ADDRESS;
use lore_telemetry::tracing::fields::BRANCH_ID;
use lore_telemetry::tracing::fields::REVISION;
use lore_transport::grpc::address_not_found_status;
use tokio::task::JoinSet;
use tonic::Request;
use tonic::Response;
use tonic::Status;
use tracing::Instrument;
use tracing::Level;
use tracing::debug;
use tracing::instrument;
use tracing::span;
use tracing::warn;

use crate::cache::revision::store_history_step;
use crate::grpc::FilterSlowDownExt;
use crate::grpc::ServerResultExt;
use crate::grpc::extract_correlation_id;
use crate::grpc::get_authorization;
use crate::grpc::get_repository;
use crate::grpc::get_user_id;
use crate::grpc::get_write_token;
use crate::grpc::hook_error_to_status;
use crate::grpc::warn_error_to_status;
use crate::hooks::HookContext;
use crate::hooks::HookDispatcher;
use crate::hooks::HookPoint;
use crate::util::setup_execution;

pub(crate) fn extract_client_ip<T>(request: &Request<T>) -> Option<IpAddr> {
    // try to get the LAST entry from XFF metadata header (injected by ALB)
    if let Some(ip_str) = request
        .metadata()
        .get("x-forwarded-for")
        .and_then(|header_value| header_value.to_str().ok())
        .and_then(|ip_list| ip_list.rsplit(',').next()) // use the last value, if multiple are present
        .map(str::trim)
        && let Ok(ip) = ip_str.parse::<IpAddr>()
    {
        return Some(ip);
    }

    // if XFF is not available, fallback to using remote_addr()
    request.remote_addr().map(|socket_addr| socket_addr.ip())
}

#[allow(clippy::too_many_arguments)]
#[tracing::instrument(name = "BranchPush::handle", skip_all)]
pub async fn handler(
    request: Request<BranchPushRequest>,
    immutable_store: Arc<dyn lore_storage::ImmutableStore>,
    mutable_store: Arc<dyn lore_storage::MutableStore>,
    notification: Arc<dyn NotificationSender>,
    hook_dispatcher: &HookDispatcher,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
    instrument_provider: &impl InstrumentProvider,
) -> Result<Response<BranchPushResponse>, Status> {
    let user_info = get_authorization(request.extensions());
    let user_id = get_user_id(request.extensions());
    let correlation_id = extract_correlation_id(&request).unwrap_or_default();
    let repository = get_repository(request.metadata())?;

    // TODO(mjansson): Once we have authz permission model with read/write/admin
    // this should be upgraded to check for the correct permission rather than
    // hardwired to service accounts. For now used to protect while allowing mirroring
    let mut bypass_protection = false;
    if let Ok(user_info) = user_info
        && user_info.is_service_account.unwrap_or_default()
    {
        bypass_protection = true;
    }

    let client_ip: Option<String> = extract_client_ip(&request).map(|ip_addr| ip_addr.to_string());
    let req = request.into_inner();
    let branch = BranchId::from(req.branch);
    let revision = Hash::from(req.revision);
    let force = req.force;
    let fast_forward_merge = req.fast_forward_merge;

    if revision.is_zero() {
        warn!("Invalid branch push request, revision is zero");
        return Err(Status::invalid_argument("Invalid revision"));
    }

    debug!({REVISION} = %revision, bypass_protection, {BRANCH_ID} = %branch, force, fast_forward_merge,
        "Handling branch push request",
    );

    let repository = Arc::new(RepositoryContext::new_server_context(
        immutable_store,
        mutable_store,
        repository,
    ));

    let repository_id: RepositoryId = repository.id;

    let execution = setup_execution(module_path!(), correlation_id.clone(), user_id.clone());

    LORE_CONTEXT
        .scope(execution, async move {
            let mut ctx_builder = HookContext::builder()
                .correlation_id(correlation_id.clone())
                .hook_point(HookPoint::BranchPush)
                .repository(repository_id)
                .user(user_id.clone())
                .branch(branch)
                .revision(revision);

            if let Some(ip) = client_ip {
                ctx_builder = ctx_builder.metadata("client_ip", ip);
            }

            let mut hook_ctx = ctx_builder.build();

            hook_dispatcher
                .dispatch_pre(HookPoint::BranchPush, &hook_ctx)
                .map_err(hook_error_to_status)?;

            let PushResult {
                success,
                fast_forward_merged,
                revision,
                revision_number,
            } = push(
                repository.clone(),
                branch,
                revision,
                bypass_protection,
                force,
                fast_forward_merge,
                history_step_size,
                acceleration,
            )
            .await?;

            if success {
                lore_spawn!({
                    let user_id = user_id.clone();
                    async move {
                        notification
                            .branch_pushed(
                                repository_id,
                                branch,
                                &user_id,
                                revision,
                                revision_number,
                            )
                            .instrument(span!(Level::DEBUG, "publish_notification"))
                            .await;
                    }
                    .in_current_span()
                });

                // Post-hook dispatch (async, non-blocking)
                hook_ctx.set_revision_number(revision_number);
                hook_dispatcher.spawn_post(HookPoint::BranchPush, hook_ctx);
            }

            let num_branches_pushed = instrument_provider.counter("num_branches_pushed");
            num_branches_pushed.add(1, &[]);

            let message = if success {
                dispatch_response_message(
                    hook_dispatcher,
                    &correlation_id,
                    &user_id,
                    repository_id,
                    branch,
                    revision,
                    repository.clone(),
                )
                .await
            } else {
                None
            };

            Ok(Response::new(BranchPushResponse {
                success,
                fast_forward_merged,
                revision: revision.into(),
                revision_number,
                message,
            }))
        })
        .await
}

/// Pre-computes repository and branch metadata, then dispatches response hooks
/// to generate an optional message for the client.
///
/// Metadata lookup failures are silently ignored — absent metadata keys cause
/// response hooks to return an empty response.
pub(crate) async fn dispatch_response_message(
    hook_dispatcher: &HookDispatcher,
    correlation_id: &str,
    user_id: &str,
    repository_id: RepositoryId,
    branch: BranchId,
    revision: Hash,
    repository: Arc<RepositoryContext>,
) -> Option<String> {
    let mut builder = HookContext::builder()
        .correlation_id(correlation_id)
        .hook_point(HookPoint::BranchPush)
        .repository(repository_id)
        .user(user_id)
        .branch(branch)
        .revision(revision);

    // no filter_slow_down()? usage here: these reads only decorate the hook
    // context, and the push they describe has already succeeded.
    if let Ok(metadata_hash) = repository::metadata_hash(repository.clone()).await
        && let Ok(repository_metadata) =
            repository::metadata(repository.clone(), metadata_hash).await
    {
        builder = builder
            .metadata("repository_name", repository_metadata.name.clone())
            .metadata(
                "default_branch_name",
                &repository_metadata.default_branch_name,
            )
            .metadata(
                "is_default_branch",
                if repository_metadata.default_branch == branch {
                    "true"
                } else {
                    "false"
                },
            );
    }

    if let Ok(branch_meta) = branch::metadata(repository.clone(), branch).await
        && let Ok(branch_meta) =
            branch::branch_metadata(repository.clone(), branch, &branch_meta).await
    {
        builder = builder.metadata("branch_name", branch_meta.name);
    }

    let response_ctx = builder.build();
    hook_dispatcher
        .dispatch_response(HookPoint::BranchPush, &response_ctx)
        .message
}

pub struct PushResult {
    pub success: bool,
    pub fast_forward_merged: bool,
    pub revision: Hash,
    pub revision_number: u64,
}

#[allow(clippy::too_many_arguments)]
#[instrument(level = "debug", skip_all, fields(branch))]
pub async fn push(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    latest: Hash,
    bypass_protection: bool,
    force: bool,
    fast_forward_merge: bool,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
) -> Result<PushResult, Status> {
    // Zero hash is never valid to push as latest pointer
    if latest.is_zero() {
        return Err(Status::failed_precondition("invalid revision signature"));
    }

    // Check if branch is protected
    let branch_metadata = metadata(repository.clone(), branch)
        .await
        .filter_slow_down()?
        .warn_map_err(|err| Status::internal(format!("Failed to load branch metadata: {err}")))?;

    if branch_metadata.get_bool(PROTECT).unwrap_or_default() {
        if bypass_protection {
            debug!("Bypass branch protection and push to protected branch");
        } else {
            warn!("Branch push failed, branch is protected");
            return Err(Status::permission_denied("protected"));
        }
    }

    // Verify the branch has not been deleted by checking the name→id mapping
    if let Ok(branch_name) = branch::name(&branch_metadata)
        && !branch_name.is_empty()
    {
        let is_mapped = branch::load_name_to_id_local(repository.clone(), branch_name)
            .await
            .filter_slow_down()?
            .is_ok_and(|id| id == branch);
        if !is_mapped {
            debug!("Branch push rejected, name-to-id mapping missing for deleted branch");
            return Err(Status::not_found("Branch not found"));
        }
    }

    let mut current_head = load_latest(repository.clone(), branch)
        .await
        .filter_slow_down()?
        .unwrap_or_default();

    // Verify the validity of the revision to push to latest
    let state = State::deserialize(repository.clone(), latest)
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            if err.is_not_found() {
                Status::not_found(format!(
                    "Revision '{latest}' to push to latest is not found"
                ))
            } else {
                Status::internal(format!("failed to load current latest state: {err}"))
            }
        })?;

    // If the incoming revision is already the latest revision the push is a no-op
    if current_head == latest {
        return Ok(PushResult {
            success: true,
            fast_forward_merged: false,
            revision: current_head,
            revision_number: state.revision_number(),
        });
    }

    let mut new_head = latest;
    loop {
        // Verify the current latest revision is parent of the incoming revision unless the push is forced
        if current_head != state.parent_self() && !force {
            if current_head.is_zero() {
                warn!("Branch push failed, branch does not exist");
                return Err(Status::not_found("Branch not found"));
            }

            // A push of several revisions moves the head once, to the tip, when the
            // current head is on the tip's first-parent line (DR-291). It is still one
            // compare-and-swap from the head this call read, so a head that moves
            // meanwhile is a lost race and never a lost update.
            if let Some(chain) =
                chain_from_head(repository.clone(), state.clone(), current_head).await?
            {
                match push_chain(
                    repository.clone(),
                    branch,
                    state.clone(),
                    current_head,
                    chain,
                    &mut new_head,
                    history_step_size,
                    acceleration,
                )
                .await?
                {
                    ChainPush::Moved(result) => return Ok(result),
                    ChainPush::HeadMoved(previous_head) => {
                        current_head = previous_head;
                        continue;
                    }
                    ChainPush::NotFastForward => {}
                }
            }

            if !fast_forward_merge {
                return Ok(PushResult {
                    success: false,
                    fast_forward_merged: false,
                    revision: current_head,
                    revision_number: 0,
                });
            }

            // Fast-forward merge: the incoming revision's parent_self no longer matches
            // the branch head. Attempt to create a new merge revision with
            // parent_self=current_head and parent_other=incoming_revision.
            return try_fast_forward_merge(
                repository.clone(),
                branch,
                state.clone(),
                current_head,
                history_step_size,
                acceleration,
            )
            .await;
        }

        let state_parent = State::deserialize(repository.clone(), state.parent_self())
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!("Failed to load incoming state: {err}"))
            })?;

        let state_other = load_other_parent_state(repository.clone(), &state).await?;

        // Verify that all new fragments exist
        verify_fragments(
            repository.clone(),
            state_parent.clone(),
            state_other.clone(),
            state.clone(),
        )
        .await?;

        // Verify that the revision number is valid
        let revision_number = next_revision_number(
            state_parent.revision_number(),
            state_other.as_ref().map_or(0, |s| s.revision_number()),
        );

        if state.revision_number() != revision_number {
            // Rewrite the revision with a correct revision number
            state.set_revision_number(revision_number);
            let write_token = get_write_token();
            new_head = state
                .serialize(repository.clone(), &write_token)
                .await
                .filter_slow_down()?
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to serialize state: {err}"))
                })?;
        }

        let previous_head = try_store_latest(repository.clone(), branch, current_head, new_head)
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!("Failed to store new latest pointer: {err}"))
            })?;

        // Check if the compare-and-swap was successful by checking match with expected value
        if previous_head == current_head {
            // If equal it means the value was swapped, i.e the push was successful. Set the
            // new latest revision signature and break out of the loop to return success
            current_head = new_head;

            store_history_step(
                repository.clone(),
                branch,
                history_step_size,
                acceleration,
                state_parent,
                state.clone(),
            )
            .await;

            break;
        }

        // Latest pointer moved during the processing of this push call, loop and try again
        current_head = previous_head;
    }

    Ok(PushResult {
        success: true,
        fast_forward_merged: false,
        revision: current_head,
        revision_number: state.revision_number(),
    })
}

/// Attempts a server-side fast-forward merge when the target branch head has moved
/// since the client created the merge revision.
///
/// Creates a new merge revision with:
/// - `parent_self` = current branch head (target branch)
/// - `parent_other` = the incoming merge revision
///
/// Uses a three-way diff between the original merge base, the incoming revision,
/// and the current head. If conflicts are detected, returns failure so the client
/// can resolve locally.
///
/// Retries via CAS loop if the branch head moves again during processing.
#[instrument(level = "debug", skip_all)]
async fn try_fast_forward_merge(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    incoming_state: Arc<State>,
    mut current_head: Hash,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
) -> Result<PushResult, Status> {
    let incoming_revision = incoming_state.revision();
    let original_base = incoming_state.parent_self();

    debug!(
        %incoming_revision, %original_base, %current_head,
        "Attempting fast-forward merge"
    );

    // Verify that all new fragments from the incoming revision exist in the store,
    // matching the verification done in the normal push path. Without this check a
    // client could reference fragments that were never fully uploaded.
    let base_state = State::deserialize(repository.clone(), original_base)
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to load base state for fragment verification: {err}"
            ))
        })?;

    let other_parent_state = load_other_parent_state(repository.clone(), &incoming_state).await?;

    verify_fragments(
        repository.clone(),
        base_state,
        other_parent_state,
        incoming_state.clone(),
    )
    .await?;

    loop {
        // Three-way diff: base=original merge target, source=incoming merge, target=current head
        debug!(
            %original_base, %incoming_revision, %current_head,
            "Computing diff3 for fast-forward merge"
        );
        let diff_result = lore_revision::revision::diff3_collect(
            repository.clone(),
            original_base,
            incoming_revision,
            current_head,
            None,
            false,
        )
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to compute diff3 for fast-forward merge: {err}"
            ))
        })?;
        debug!(
            changes = diff_result.changes.len(),
            conflicts = diff_result.conflicts.len(),
            "diff3 result for fast-forward merge"
        );

        if !diff_result.conflicts.is_empty() {
            debug!(
                conflicts = diff_result.conflicts.len(),
                "Fast-forward merge has conflicts, rejecting"
            );
            return Ok(PushResult {
                success: false,
                fast_forward_merged: false,
                revision: current_head,
                revision_number: 0,
            });
        }

        // Deserialize the current head state to use as base for the new merge revision
        let state_current = State::deserialize(repository.clone(), current_head)
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!(
                    "Failed to deserialize current head for fast-forward merge: {err}"
                ))
            })?;

        // Apply the non-conflicting changes to the current head state
        state::apply_tree_changes(
            repository.clone(),
            state_current.clone(),
            &diff_result.changes,
        )
        .await
        .warn_map_err(|err| {
            Status::internal(format!(
                "Failed to apply tree changes for fast-forward merge: {err}"
            ))
        })?;

        // Set parents: self=current head (target branch), other=incoming merge revision
        state_current.set_parent_self(current_head);
        state_current.set_parent_other(incoming_revision);

        // Compute revision number from both parents
        let parent_state = State::deserialize(repository.clone(), current_head)
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!("Failed to load current head state: {err}"))
            })?;

        let revision_number = next_revision_number(
            parent_state.revision_number(),
            incoming_state.revision_number(),
        );
        state_current.set_revision_number(revision_number);

        // Copy metadata from the incoming revision and set merged-by to "server"
        let incoming_metadata_hash = incoming_state.metadata_hash();
        if !incoming_metadata_hash.is_zero() {
            let mut metadata = lore_revision::metadata::Metadata::deserialize(
                repository.clone(),
                incoming_metadata_hash,
            )
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!("Failed to load incoming revision metadata: {err}"))
            })?;

            metadata
                .set_branch(branch)
                .warn_map_err(|_| Status::internal("Failed to set branch in metadata"))?;
            // Preserve the existing merged-by field if set, otherwise fall back to "server"
            if metadata
                .get_string(lore_revision::metadata::MERGED_BY)
                .is_err()
            {
                metadata
                    .set_string(lore_revision::metadata::MERGED_BY, "server")
                    .warn_map_err(|_| Status::internal("Failed to set merged-by in metadata"))?;
            }
            metadata
                .set_u64(lore_revision::metadata::FAST_FORWARD_MERGE, 1)
                .warn_map_err(|_| {
                    Status::internal("Failed to set fast-forward-merge in metadata")
                })?;

            let metadata_hash = metadata
                .serialize(repository.clone())
                .await
                .filter_slow_down()?
                .warn_map_err(|_| Status::internal("Failed to serialize metadata"))?;
            state_current.set_metadata_hash(metadata_hash);
        }

        // Serialize the new merge state
        let write_token = get_write_token();
        let new_revision = state_current
            .serialize(repository.clone(), &write_token)
            .await
            .filter_slow_down()?
            .warn_map_err(|err| {
                Status::internal(format!(
                    "Failed to serialize fast-forward merge state: {err}"
                ))
            })?;

        // CAS: attempt to set the new revision as branch head
        let previous_head =
            try_store_latest(repository.clone(), branch, current_head, new_revision)
                .await
                .filter_slow_down()?
                .warn_map_err(|err| {
                    Status::internal(format!("Failed to store fast-forward merge latest: {err}"))
                })?;

        if previous_head == current_head {
            // CAS succeeded
            debug!(
                %new_revision, revision_number,
                "Fast-forward merge succeeded"
            );

            // Store acceleration index if needed
            store_history_step(
                repository.clone(),
                branch,
                history_step_size,
                acceleration,
                parent_state,
                state_current.clone(),
            )
            .await;

            return Ok(PushResult {
                success: true,
                fast_forward_merged: true,
                revision: new_revision,
                revision_number,
            });
        }

        // CAS failed — branch head moved again, retry with updated head
        debug!(
            %previous_head, %current_head,
            "Fast-forward merge CAS failed, retrying"
        );
        current_head = previous_head;
    }
}

/// Compute the next revision number from the parent revision numbers.
/// The revision number is one greater than the maximum of the two parents.
fn next_revision_number(parent_self_number: u64, parent_other_number: u64) -> u64 {
    std::cmp::max(parent_self_number, parent_other_number) + 1
}

/// What [`push_chain`] did with a push of several revisions.
enum ChainPush {
    /// The head moved from where the chain starts to the tip.
    Moved(PushResult),
    /// The head was no longer where the chain starts when the swap ran. Carries
    /// the head the swap found.
    HeadMoved(Hash),
    /// A revision below the tip carries the wrong revision number. Correcting it
    /// would change it and every revision above it, so the push is answered as a
    /// single step would be: not a fast-forward.
    NotFastForward,
}

/// The revisions from `head` up to the parent of `tip`, oldest first and
/// including `head`, when `head` is on `tip`'s first-parent line and the push
/// moves it over at most [`push::MAX_PUSH_CHAIN`] revisions.
///
/// `None` when it is not, which is the single-step answer: not a fast-forward.
/// A revision the store does not hold ends the walk the same way, so a push that
/// is not a provable chain is refused exactly as it was before; a client that
/// pushed a backlog then pushes it one revision at a time, and that path names
/// what is missing.
///
/// The walk stops as soon as it passes below `head`'s revision number. A first
/// parent always numbers lower than its child, so nothing past that point can be
/// `head`, and a push from an unrelated line costs a read or two rather than a
/// walk of its whole history.
async fn chain_from_head(
    repository: Arc<RepositoryContext>,
    tip: Arc<State>,
    head: Hash,
) -> Result<Option<Vec<Arc<State>>>, Status> {
    let Some(head_state) = load_held_state(&repository, head).await? else {
        return Ok(None);
    };
    let floor = head_state.revision_number();

    let mut chain = Vec::new();
    let mut cursor = tip.parent_self();
    while cursor != head {
        // `chain` holds the revisions below the tip, so the tip makes one more.
        if cursor.is_zero() || chain.len() + 1 >= push::MAX_PUSH_CHAIN {
            return Ok(None);
        }
        let Some(state) = load_held_state(&repository, cursor).await? else {
            return Ok(None);
        };
        if state.revision_number() <= floor {
            return Ok(None);
        }
        cursor = state.parent_self();
        chain.push(state);
    }
    chain.push(head_state);
    chain.reverse();
    Ok(Some(chain))
}

/// The state at `revision`, or `None` when the store does not hold it.
async fn load_held_state(
    repository: &Arc<RepositoryContext>,
    revision: Hash,
) -> Result<Option<Arc<State>>, Status> {
    match State::deserialize(repository.clone(), revision)
        .await
        .filter_slow_down()?
    {
        Ok(state) => Ok(Some(state)),
        Err(err) if err.is_not_found() => Ok(None),
        Err(err) => Err(warn_error_to_status(&err, |err| {
            Status::internal(format!("Failed to load revision {revision}: {err}"))
        })),
    }
}

/// Move the branch head from `head` to `tip` in one compare-and-swap, having
/// checked every step between them as a push of that one revision would.
///
/// `chain` is [`chain_from_head`]'s answer: `head` first, then each revision
/// above it, up to `tip`'s parent. Each step is verified against its own parents
/// for missing fragments and for its revision number, then the history-step
/// index is written per step so it names the same revisions a push of one
/// revision at a time would have left it naming.
///
/// Only the tip may have its revision number rewritten, as a single push does,
/// and `new_head` follows it. A wrong number below the tip is
/// [`ChainPush::NotFastForward`].
#[allow(clippy::too_many_arguments)]
async fn push_chain(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    tip: Arc<State>,
    head: Hash,
    chain: Vec<Arc<State>>,
    new_head: &mut Hash,
    history_step_size: u64,
    acceleration: crate::grpc::server::RevisionListAcceleration,
) -> Result<ChainPush, Status> {
    let mut steps = Vec::with_capacity(chain.len());
    for (at, parent) in chain.iter().enumerate() {
        let child = chain.get(at + 1).cloned().unwrap_or_else(|| tip.clone());
        let other = load_other_parent_state(repository.clone(), &child).await?;
        steps.push((parent.clone(), other, child));
    }

    let number_of = |parent: &Arc<State>, other: &Option<Arc<State>>| {
        next_revision_number(
            parent.revision_number(),
            other.as_ref().map_or(0, |s| s.revision_number()),
        )
    };

    let (below_tip, [(tip_parent, tip_other, _)]) = steps.split_at(steps.len() - 1) else {
        unreachable!("a chain holds at least the head");
    };
    if let Some((parent, _, child)) = below_tip
        .iter()
        .find(|(parent, other, child)| child.revision_number() != number_of(parent, other))
    {
        debug!(
            revision = %child.revision(), parent = %parent.revision(),
            "Branch push of several revisions refused, a revision below the tip is misnumbered"
        );
        return Ok(ChainPush::NotFastForward);
    }

    for (parent, other, child) in &steps {
        verify_fragments(
            repository.clone(),
            parent.clone(),
            other.clone(),
            child.clone(),
        )
        .await?;
    }

    let revision_number = number_of(tip_parent, tip_other);
    if tip.revision_number() != revision_number {
        tip.set_revision_number(revision_number);
        let write_token = get_write_token();
        *new_head = tip
            .serialize(repository.clone(), &write_token)
            .await
            .filter_slow_down()?
            .warn_map_err(|err| Status::internal(format!("Failed to serialize state: {err}")))?;
    }

    debug!(
        %head, tip = %new_head, steps = steps.len(),
        "Moving the branch head over several revisions"
    );
    let previous_head = try_store_latest(repository.clone(), branch, head, *new_head)
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            Status::internal(format!("Failed to store new latest pointer: {err}"))
        })?;
    if previous_head != head {
        return Ok(ChainPush::HeadMoved(previous_head));
    }

    for (parent, _, child) in steps {
        store_history_step(
            repository.clone(),
            branch,
            history_step_size,
            acceleration,
            parent,
            child,
        )
        .await;
    }

    Ok(ChainPush::Moved(PushResult {
        success: true,
        fast_forward_merged: false,
        revision: *new_head,
        revision_number,
    }))
}

/// The first address in `batch` the store did not answer with a full match,
/// warned where it is found.
fn first_missing_fragment(batch: &[Address], answers: &[StoreMatchResult]) -> Option<Address> {
    let address = batch
        .iter()
        .zip(answers.iter())
        .find(|(_, answer)| answer.match_made != StoreMatch::MatchFull)
        .map(|(address, _)| *address)?;

    warn!({ADDRESS} = %address, "Branch push failed, fragment not found");
    Some(address)
}

/// The state of the second parent `state` names, or `None` when it names none.
///
/// Only a merge revision names one, and both the revision number the merge takes and the
/// fragments it is verified against come from that parent. A parent the store cannot
/// answer for is reported as `FAILED_PRECONDITION` naming its address, as a missing
/// fragment is. The address is the one the load asked for, since a state the store cannot
/// answer for is reported as a plain absence carrying no address of its own.
async fn load_other_parent_state(
    repository: Arc<RepositoryContext>,
    state: &State,
) -> Result<Option<Arc<State>>, Status> {
    if state.parent_other().is_zero() {
        return Ok(None);
    }

    let address = Address::zero_context_hash(state.parent_other());
    let other_parent_state = State::deserialize(repository, state.parent_other())
        .await
        .filter_slow_down()?
        .warn_map_err(|err| {
            if err.is_not_found() {
                return address_not_found_status(
                    &AddressNotFound::from(address),
                    format!("Missing fragment '{address}'"),
                );
            }

            Status::internal(format!("Failed to load other parent state: {err}"))
        })?;

    Ok(Some(other_parent_state))
}

/// Verify that all new fragments between `parent_state` and `state` exist in the
/// immutable store. Also includes the other parent hash if the state is a merge.
/// Returns an error if any fragment is missing.
///
/// A merge revision joins two lines of history, and what it names is new to this
/// branch against either of them, so `other_parent_state` is collected against as well.
/// Reading that parent's own tree is what refuses a peer that pushed the merge without
/// the tip of the line it merged.
///
/// A missing fragment is reported as `FAILED_PRECONDITION` naming the address,
/// whether the walk cannot read it or the store answers that it is absent.
/// `NOT_FOUND` is left to name an absent branch, which a caller reinstates.
async fn verify_fragments(
    repository: Arc<RepositoryContext>,
    parent_state: Arc<State>,
    other_parent_state: Option<Arc<State>>,
    state: Arc<State>,
) -> Result<(), Status> {
    let collect = async |parent_state: Arc<State>| {
        state::collect_new_fragments(
            repository.clone(),
            parent_state,
            state.clone(),
            true, /* Ignore already durably stored fragments */
        )
        .instrument(span!(Level::DEBUG, "collect_new_fragments"))
        .await
        .warn_map_err(|err| {
            if let Some(converted_error) = err.as_address_not_found() {
                return address_not_found_status(
                    converted_error,
                    format!(
                        "Failed to collect new fragments for verification. Missing address '{converted_error}'"
                    ),
                );
            }

            Status::internal(format!(
                "Failed to collect new fragments for verification: {err}"
            ))
        })
    };

    let mut new_fragments = collect(parent_state).await?;

    if let Some(other_parent_state) = other_parent_state {
        new_fragments.append(&mut collect(other_parent_state).await?);
        new_fragments.push(Address::zero_context_hash(state.parent_other()));
    }

    new_fragments.sort_unstable();
    new_fragments.dedup();

    let mut retry = lore_revision::util::time::retry(
        push::RETRY_START_DURATION,
        push::RETRY_MAX_DURATION,
        push::RETRY_MAX_ATTEMPTS,
    );

    let max_batch_size = repository
        .immutable_store()
        .max_query_batch()
        .unwrap_or(1000)
        .clamp(100, 10000);

    let mut tasks = JoinSet::new();
    while !new_fragments.is_empty() || !tasks.is_empty() {
        let batch_span = span!(
            Level::DEBUG,
            "exist_batch",
            items = new_fragments.len(),
            batch_size = max_batch_size
        );

        batch_span.in_scope(|| {
            while !new_fragments.is_empty() {
                let repository = repository.clone();
                let batch =
                    new_fragments.split_off(new_fragments.len().saturating_sub(max_batch_size));
                lore_spawn!(
                    tasks,
                    async move {
                        let mut resolved = vec![StoreMatchResult::default(); batch.len()];
                        let result = repository
                            .immutable_store()
                            .query(repository.id, batch.as_slice(), &mut resolved)
                            .await
                            .map(|()| resolved);
                        (batch, result)
                    }
                    .in_current_span()
                );
            }
        });

        let mut num_slow_downs = 0;
        while let Some(result) = tasks.join_next().await {
            let (mut batch, result) =
                result.warn_map_err(|err| Status::internal(format!("Query task failed: {err}")))?;
            match result {
                Ok(result) => {
                    if let Some(missing) = first_missing_fragment(&batch, &result) {
                        return Err(address_not_found_status(
                            &AddressNotFound::from(missing),
                            format!("Missing fragment '{missing}'"),
                        ));
                    }
                }
                Err(StoreError::SlowDown(_)) => {
                    new_fragments.append(&mut batch);
                    num_slow_downs += 1;
                }
                Err(err) => {
                    let response = warn_error_to_status(&err, |err| {
                        Status::internal(format!("Store query failed: {err}"))
                    });
                    return Err(response);
                }
            }
        }

        if !new_fragments.is_empty() && !retry.wait().await {
            warn!("Exhausted {num_slow_downs} fragment exist retries");
            return Err(Status::resource_exhausted("Slow down"));
        }
    }

    Ok(())
}

#[instrument(level = "debug", skip_all, fields(branch))]
pub async fn try_store_latest(
    repository: Arc<RepositoryContext>,
    branch: BranchId,
    current_expected_latest: Hash,
    new_latest: Hash,
) -> Result<Hash, BranchError> {
    lore_revision::branch::mutable_try_store(
        repository.clone(),
        LATEST,
        branch,
        current_expected_latest,
        new_latest,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::net::Ipv4Addr;
    use std::net::SocketAddr;

    use lore_revision::branch::DEFAULT_HISTORY_STEP_SIZE;
    use lore_revision::node::Node;
    use lore_revision::node::NodeFlags;
    use lore_revision::node::ROOT_NODE;
    use rand::random;
    use tonic::Code;
    use tonic::Request;
    use tonic::metadata::MetadataValue;
    use tonic::transport::server::TcpConnectInfo;

    use super::*;
    use crate::grpc::server::RevisionListAcceleration;
    use crate::store::test_store_create;

    async fn create_test_branch(repository: &Arc<RepositoryContext>) -> BranchId {
        let branch_id = BranchId::from(uuid::Uuid::now_v7());
        let write_token = get_write_token();
        branch::create(
            repository.clone(),
            &write_token,
            branch_id,
            "test-branch",
            branch::default_category(),
            "creator",
            1,
            vec![],
            false,
            false,
        )
        .await
        .expect("create branch");
        branch_id
    }

    async fn serialize_revision(
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
        parent_self: Hash,
        parent_other: Hash,
        revision_number: u64,
    ) -> Arc<State> {
        let write_token = get_write_token();
        let mut metadata = lore_revision::metadata::Metadata::new();
        metadata.set_branch(branch).expect("set branch");
        let metadata_hash = metadata
            .serialize(repository.clone())
            .await
            .expect("serialize metadata");

        let state = Arc::new(State::new());
        state.set_parent_self(parent_self);
        if !parent_other.is_zero() {
            state.set_parent_other(parent_other);
        }
        state.set_revision_number(revision_number);
        state.set_metadata_hash(metadata_hash);
        state
            .serialize(repository.clone(), &write_token)
            .await
            .expect("serialize state");
        state
    }

    /// A revision whose metadata names `payload`, so the walk collects an address
    /// it never reads.
    async fn serialize_revision_naming_a_payload(
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
        payload: Address,
    ) -> Arc<State> {
        let write_token = get_write_token();
        let mut metadata = lore_revision::metadata::Metadata::new();
        metadata.set_branch(branch).expect("set branch");
        metadata
            .set_address("build-artifact", payload)
            .expect("set the payload address");
        let metadata_hash = metadata
            .serialize(repository.clone())
            .await
            .expect("serialize metadata");

        let state = Arc::new(State::new());
        state.set_parent_self(Hash::default());
        state.set_revision_number(1);
        state.set_metadata_hash(metadata_hash);
        state
            .serialize(repository.clone(), &write_token)
            .await
            .expect("serialize state");
        state
    }

    /// A revision holding one file, so its state references node and name
    /// fragments the walk has to read.
    async fn serialize_revision_with_a_file(
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
    ) -> Arc<State> {
        let write_token = get_write_token();
        let mut metadata = lore_revision::metadata::Metadata::new();
        metadata.set_branch(branch).expect("set branch");
        let metadata_hash = metadata
            .serialize(repository.clone())
            .await
            .expect("serialize metadata");

        let state = Arc::new(State::new());
        state.set_parent_self(Hash::default());
        state.set_revision_number(1);
        state.set_metadata_hash(metadata_hash);
        state
            .node_add(
                repository.clone(),
                ROOT_NODE,
                Node {
                    flags: NodeFlags::File.bits(),
                    name_hash: lore_storage::hash::hash_string("file.txt"),
                    ..Default::default()
                },
                "file.txt",
            )
            .await
            .expect("node_add");
        state
            .serialize(repository.clone(), &write_token)
            .await
            .expect("serialize state");
        state
    }

    /// Copy the fragment at `hash` alone, leaving everything it references absent
    /// in `target`.
    async fn hand_over_fragment(
        source: &Arc<dyn lore_storage::ImmutableStore>,
        target: &Arc<dyn lore_storage::ImmutableStore>,
        repository: RepositoryId,
        hash: Hash,
    ) {
        let address = Address::zero_context_hash(hash);
        let data = source
            .clone()
            .get(repository, address)
            .await
            .expect("read the fragment to hand over");
        target
            .clone()
            .put(repository, address, data.fragment, data.payload, false)
            .await
            .expect("hand over the fragment");
    }

    /// Push revisions `numbers`, chained from `parent`. Returns the pushed
    /// signatures oldest-first.
    async fn push_linear_revisions(
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
        parent: Hash,
        numbers: std::ops::RangeInclusive<u64>,
    ) -> Vec<Hash> {
        let mut parent = parent;
        let mut signatures = Vec::new();
        for number in numbers {
            let state =
                serialize_revision(repository, branch, parent, Hash::default(), number).await;
            parent = push(
                repository.clone(),
                branch,
                state.revision(),
                true,
                true,
                false,
                DEFAULT_HISTORY_STEP_SIZE,
                RevisionListAcceleration::default(),
            )
            .await
            .expect("push revision")
            .revision;
            signatures.push(parent);
        }
        signatures
    }

    /// Push a merge revision whose `parent_other` carries a much higher
    /// revision number, so the branch's revision number jumps to
    /// `other_revision_number + 1` and skips the numbers in between.
    async fn push_jump_revision(
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
        parent: Hash,
        other_revision_number: u64,
    ) -> (Hash, u64) {
        let other = serialize_revision(
            repository,
            branch,
            Hash::default(),
            Hash::default(),
            other_revision_number,
        )
        .await;
        let state = serialize_revision(
            repository,
            branch,
            parent,
            other.revision(),
            0, /* rewritten */
        )
        .await;

        let result = push(
            repository.clone(),
            branch,
            state.revision(),
            true,
            true,
            false,
            DEFAULT_HISTORY_STEP_SIZE,
            RevisionListAcceleration::default(),
        )
        .await
        .expect("push jump revision");
        (result.revision, result.revision_number)
    }

    /// Read the revision sealed at `boundary`, or `None` when unsealed.
    async fn load_step_key(
        repository: &Arc<RepositoryContext>,
        branch: BranchId,
        boundary: u64,
    ) -> Option<Hash> {
        let (key, key_type) = branch::revision_step_key(
            repository::SALT_LORE,
            repository.id,
            branch,
            boundary,
            DEFAULT_HISTORY_STEP_SIZE,
        );
        repository
            .clone()
            .read_mutable_store()
            .load(repository.id, key, key_type)
            .await
            .ok()
            .filter(|revision| !revision.is_zero())
    }

    mod extract_client_ip {
        use super::*;

        #[test]
        fn use_x_forwarded_when_available() {
            let mut req = Request::new(());

            let xff_metadata_value: MetadataValue<_> = "10.0.0.1, 10.0.0.2".parse().unwrap();
            req.metadata_mut()
                .insert("x-forwarded-for", xff_metadata_value);

            // set remote address to make sure it's NOT used in presence of the XFF header
            let peer_addr = SocketAddr::from(([192, 168, 1, 42], 4242));
            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(peer_addr),
            });

            assert_eq!(
                extract_client_ip(&req),
                Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2)))
            );
        }

        #[test]
        fn dont_use_xff_when_it_contains_invalid_value() {
            let mut req = Request::new(());

            let xff_metadata_value: MetadataValue<_> = "10.0.0.lol, 10.0.0.wat".parse().unwrap();
            req.metadata_mut()
                .insert("x-forwarded-for", xff_metadata_value);

            let peer_addr = SocketAddr::from(([192, 168, 1, 42], 4242));
            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(peer_addr),
            });

            assert_eq!(
                extract_client_ip(&req),
                Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42)))
            );
        }

        #[test]
        fn still_uses_last_ip_when_xff_contains_invalid_value_in_chain() {
            let mut req = Request::new(());

            let xff_metadata_value: MetadataValue<_> =
                "10.0.0.lol, 10.0.0.wat, 10.0.0.42".parse().unwrap();
            req.metadata_mut()
                .insert("x-forwarded-for", xff_metadata_value);

            let peer_addr = SocketAddr::from(([192, 168, 1, 42], 4242));
            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(peer_addr),
            });

            assert_eq!(
                extract_client_ip(&req),
                Some(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 42)))
            );
        }

        #[test]
        fn fallback_to_remote_addr() {
            let mut req = Request::new(());

            let peer_addr = SocketAddr::from(([192, 168, 1, 42], 31415));
            req.extensions_mut().insert(TcpConnectInfo {
                local_addr: None,
                remote_addr: Some(peer_addr),
            });

            assert_eq!(
                extract_client_ip(&req),
                Some(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 42)))
            );
        }
    }

    mod push {
        use super::*;

        #[tokio::test]
        async fn push_unknown_revision_returns_not_found() {
            let repository_id = random::<RepositoryId>();
            let branch_id = BranchId::from(uuid::Uuid::now_v7());

            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
                let repository_context = Arc::new(RepositoryContext::new_server_context(
                    immutable_store,
                    mutable_store,
                    repository_id,
                ));

                let write_token = get_write_token();
                branch::create(
                    repository_context.clone(),
                    &write_token,
                    branch_id,
                    "test-branch",
                    branch::personal_category(),
                    "test-creator",
                    1,
                    vec![],
                    false,
                    false,
                )
                .await
                .expect("Failed to create branch");

                // A hash with no corresponding state data in the immutable store
                let nonexistent_revision = random::<Hash>();

                let result = push(
                    repository_context,
                    branch_id,
                    nonexistent_revision,
                    true,
                    true,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    RevisionListAcceleration::default(),
                )
                .await;

                let Err(status) = result else {
                    panic!("an unknown revision cannot be pushed");
                };
                assert_eq!(status.code(), Code::NotFound);
                let error = lore_transport::ProtocolError::from(status);
                assert!(error.is_not_found(), "{error:?}");
            }))
            .await;
        }

        /// A fragment the walk cannot read is named as an address the caller
        /// reconstructs. Both detections share a code, so the message is what
        /// pins which one this reaches.
        #[tokio::test]
        async fn a_fragment_the_walk_cannot_read_names_its_address() {
            let repository_id = random::<RepositoryId>();

            let (peer_store, peer_mutable, execution) =
                test_store_create().await.expect("Failed to create stores");
            let (store, mutable_store, _) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let peer = Arc::new(RepositoryContext::new_server_context(
                    peer_store.clone(),
                    peer_mutable,
                    repository_id,
                ));
                let repository = Arc::new(RepositoryContext::new_server_context(
                    store.clone(),
                    mutable_store,
                    repository_id,
                ));

                let branch = create_test_branch(&repository).await;
                let state = serialize_revision_with_a_file(&peer, branch).await;

                hand_over_fragment(&peer_store, &store, repository_id, state.revision()).await;

                let Err(status) = push(
                    repository,
                    branch,
                    state.revision(),
                    true,
                    true,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    RevisionListAcceleration::default(),
                )
                .await
                else {
                    panic!("a revision missing its fragments cannot be pushed");
                };

                assert_eq!(status.code(), Code::FailedPrecondition);
                assert!(
                    status
                        .message()
                        .starts_with("Failed to collect new fragments"),
                    "{}",
                    status.message()
                );
                let error = lore_transport::ProtocolError::from(status);
                assert!(error.is_address_not_found(), "{error:?}");
            }))
            .await;
        }

        /// A fragment the store answers as absent is named the same way, so the
        /// two paths that detect it report one condition.
        ///
        /// The revision and the blob holding its metadata are both handed over,
        /// since the walk reads both. What stays absent is the payload that
        /// metadata names, which the walk collects without reading, so the store
        /// query is what detects it.
        #[tokio::test]
        async fn a_fragment_the_store_reports_absent_names_its_address() {
            let repository_id = random::<RepositoryId>();

            let (peer_store, peer_mutable, execution) =
                test_store_create().await.expect("Failed to create stores");
            let (store, mutable_store, _) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let peer = Arc::new(RepositoryContext::new_server_context(
                    peer_store.clone(),
                    peer_mutable,
                    repository_id,
                ));
                let repository = Arc::new(RepositoryContext::new_server_context(
                    store.clone(),
                    mutable_store,
                    repository_id,
                ));

                let branch = create_test_branch(&repository).await;
                let payload = Address::zero_context_hash(Hash::from([0xabu8; 32]));
                let state = serialize_revision_naming_a_payload(&peer, branch, payload).await;
                hand_over_fragment(&peer_store, &store, repository_id, state.revision()).await;
                hand_over_fragment(&peer_store, &store, repository_id, state.metadata_hash()).await;

                let Err(status) = push(
                    repository,
                    branch,
                    state.revision(),
                    true,
                    true,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    RevisionListAcceleration::default(),
                )
                .await
                else {
                    panic!("a revision missing a payload its metadata names cannot be pushed");
                };

                assert_eq!(status.code(), Code::FailedPrecondition);
                assert_eq!(
                    status.message(),
                    format!("Missing fragment '{payload}'"),
                    "the absent payload has to be the fragment named"
                );
                let error = lore_transport::ProtocolError::from(status);
                assert!(error.is_address_not_found(), "{error:?}");
            }))
            .await;
        }

        /// A merge whose second parent the store cannot answer for is refused as the
        /// missing fragment it is.
        ///
        /// The merge and the blob holding its metadata are handed over, so what stays
        /// absent is the line the merge joins. That parent carries what the merge is
        /// verified against, so it is read rather than only queried, and the read has to
        /// report the address as a missing fragment like the query does.
        #[tokio::test]
        async fn a_merge_missing_its_other_parent_names_that_address() {
            let repository_id = random::<RepositoryId>();

            let (peer_store, peer_mutable, execution) =
                test_store_create().await.expect("Failed to create stores");
            let (store, mutable_store, _) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let peer = Arc::new(RepositoryContext::new_server_context(
                    peer_store.clone(),
                    peer_mutable,
                    repository_id,
                ));
                let repository = Arc::new(RepositoryContext::new_server_context(
                    store.clone(),
                    mutable_store,
                    repository_id,
                ));

                let branch = create_test_branch(&repository).await;
                let other =
                    serialize_revision(&peer, branch, Hash::default(), Hash::default(), 1).await;
                let merge =
                    serialize_revision(&peer, branch, Hash::default(), other.revision(), 2).await;

                hand_over_fragment(&peer_store, &store, repository_id, merge.revision()).await;
                hand_over_fragment(&peer_store, &store, repository_id, merge.metadata_hash()).await;

                let Err(status) = push(
                    repository,
                    branch,
                    merge.revision(),
                    true,
                    true,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    RevisionListAcceleration::default(),
                )
                .await
                else {
                    panic!("a merge missing the line it joins cannot be pushed");
                };

                assert_eq!(status.code(), Code::FailedPrecondition);
                assert_eq!(
                    status.message(),
                    format!(
                        "Missing fragment '{}'",
                        Address::zero_context_hash(other.revision())
                    ),
                    "the absent parent has to be the fragment named"
                );
                let error = lore_transport::ProtocolError::from(status);
                assert!(error.is_address_not_found(), "{error:?}");
            }))
            .await;
        }

        /// A merge is verified against its second parent as well as its first, so a
        /// second parent the store cannot walk refuses the push.
        ///
        /// That parent and the blob holding its metadata are handed over, leaving the tree
        /// it names absent. The merge itself holds no file, so collecting it against its
        /// first parent reads nothing of that tree: what reaches it is the collection
        /// against the second parent, which is the one this covers.
        #[tokio::test]
        async fn a_merge_is_verified_against_the_tree_of_its_other_parent() {
            let repository_id = random::<RepositoryId>();

            let (peer_store, peer_mutable, execution) =
                test_store_create().await.expect("Failed to create stores");
            let (store, mutable_store, _) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let peer = Arc::new(RepositoryContext::new_server_context(
                    peer_store.clone(),
                    peer_mutable,
                    repository_id,
                ));
                let repository = Arc::new(RepositoryContext::new_server_context(
                    store.clone(),
                    mutable_store,
                    repository_id,
                ));

                let branch = create_test_branch(&repository).await;
                let other = serialize_revision_with_a_file(&peer, branch).await;
                let merge =
                    serialize_revision(&peer, branch, Hash::default(), other.revision(), 2).await;

                for hash in [
                    merge.revision(),
                    merge.metadata_hash(),
                    other.revision(),
                    other.metadata_hash(),
                ] {
                    hand_over_fragment(&peer_store, &store, repository_id, hash).await;
                }

                let Err(status) = push(
                    repository,
                    branch,
                    merge.revision(),
                    true,
                    true,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    RevisionListAcceleration::default(),
                )
                .await
                else {
                    panic!("a merge whose other parent cannot be walked cannot be pushed");
                };

                assert_eq!(status.code(), Code::FailedPrecondition);
                assert!(
                    status
                        .message()
                        .starts_with("Failed to collect new fragments"),
                    "{}",
                    status.message()
                );
                let error = lore_transport::ProtocolError::from(status);
                assert!(error.is_address_not_found(), "{error:?}");
            }))
            .await;
        }

        #[tokio::test]
        async fn linear_history_seals_a_boundary_only_once_the_head_moves_past_it() {
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository = Arc::new(RepositoryContext::new_server_context(
                    immutable_store,
                    mutable_store,
                    random::<RepositoryId>(),
                ));
                let branch = create_test_branch(&repository).await;

                let chain =
                    push_linear_revisions(&repository, branch, Hash::default(), 1..=100).await;

                // Revision 100 is the head, so segment 100 is still the open one.
                assert_eq!(load_step_key(&repository, branch, 100).await, None);

                push_linear_revisions(&repository, branch, chain[99], 101..=101).await;

                // Now the head has moved past 100, sealing it with revision 100.
                assert_eq!(
                    load_step_key(&repository, branch, 100).await,
                    Some(chain[99])
                );
                // Nothing above the head may be sealed.
                assert_eq!(load_step_key(&repository, branch, 200).await, None);
            }))
            .await;
        }

        #[tokio::test]
        async fn linear_history_seals_each_boundary_with_its_own_highest_revision() {
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository = Arc::new(RepositoryContext::new_server_context(
                    immutable_store,
                    mutable_store,
                    random::<RepositoryId>(),
                ));
                let branch = create_test_branch(&repository).await;

                let chain =
                    push_linear_revisions(&repository, branch, Hash::default(), 1..=250).await;

                assert_eq!(
                    load_step_key(&repository, branch, 100).await,
                    Some(chain[99])
                );
                assert_eq!(
                    load_step_key(&repository, branch, 200).await,
                    Some(chain[199])
                );
                // Segment 300 holds the head at 250 and stays open.
                assert_eq!(load_step_key(&repository, branch, 300).await, None);
            }))
            .await;
        }

        /// A jump seals the boundaries between the two revisions and no
        /// others. The segment the new revision lands in stays open, since the
        /// revisions above it do not exist yet.
        #[tokio::test]
        async fn jump_seals_the_crossed_boundary_and_not_the_one_it_landed_in() {
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository = Arc::new(RepositoryContext::new_server_context(
                    immutable_store,
                    mutable_store,
                    random::<RepositoryId>(),
                ));
                let branch = create_test_branch(&repository).await;

                let chain =
                    push_linear_revisions(&repository, branch, Hash::default(), 1..=99).await;
                let (_, revision_number) =
                    push_jump_revision(&repository, branch, chain[98], 104).await;
                assert_eq!(revision_number, 105);

                // Boundary 100 is the only one crossed, answered by revision 99.
                assert_eq!(
                    load_step_key(&repository, branch, 100).await,
                    Some(chain[98])
                );
                // Segment 200 contains the new head at 105 and is still open.
                assert_eq!(load_step_key(&repository, branch, 200).await, None);
                assert_eq!(load_step_key(&repository, branch, 300).await, None);
            }))
            .await;
        }

        #[tokio::test]
        async fn jump_seals_every_boundary_it_skipped_over() {
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let repository = Arc::new(RepositoryContext::new_server_context(
                    immutable_store,
                    mutable_store,
                    random::<RepositoryId>(),
                ));
                let branch = create_test_branch(&repository).await;

                let chain =
                    push_linear_revisions(&repository, branch, Hash::default(), 1..=150).await;
                assert_eq!(
                    load_step_key(&repository, branch, 100).await,
                    Some(chain[99])
                );

                let (_, revision_number) =
                    push_jump_revision(&repository, branch, chain[149], 399).await;
                assert_eq!(revision_number, 400);

                // 150 -> 400 skips 200 and 300; both are answered by revision 150,
                // the highest revision numbered at or below them.
                assert_eq!(
                    load_step_key(&repository, branch, 200).await,
                    Some(chain[149])
                );
                assert_eq!(
                    load_step_key(&repository, branch, 300).await,
                    Some(chain[149])
                );
                // The boundary already sealed before the jump is left alone.
                assert_eq!(
                    load_step_key(&repository, branch, 100).await,
                    Some(chain[99])
                );
                // Segment 400 holds the new head, and 500 was never reached.
                assert_eq!(load_step_key(&repository, branch, 400).await, None);
                assert_eq!(load_step_key(&repository, branch, 500).await, None);
            }))
            .await;
        }
    }

    /// DR-291: a push of several revisions moves the branch head once.
    mod push_backlog {
        use std::sync::atomic::AtomicUsize;
        use std::sync::atomic::Ordering;

        use lore_storage::KeyType;
        use lore_storage::Partition;
        use lore_storage::store_types::KeyValueStream;

        use super::*;

        /// A mutable store that counts the compare-and-swaps it is asked for, and can
        /// play a second writer that wins the next one from the same expected head.
        struct WatchedStore {
            inner: Arc<dyn lore_storage::MutableStore>,
            swaps: AtomicUsize,
            rival: std::sync::Mutex<Option<Hash>>,
        }

        impl WatchedStore {
            fn swaps(&self) -> usize {
                self.swaps.load(Ordering::SeqCst)
            }

            fn forget_swaps(&self) {
                self.swaps.store(0, Ordering::SeqCst);
            }

            /// The next compare-and-swap finds `rival` already swapped in.
            fn race_with(&self, rival: Hash) {
                *self.rival.lock().unwrap() = Some(rival);
            }
        }

        #[async_trait::async_trait]
        impl lore_storage::MutableStore for WatchedStore {
            async fn load(
                self: Arc<Self>,
                partition: Partition,
                key: Hash,
                key_type: KeyType,
            ) -> Result<Hash, StoreError> {
                self.inner.clone().load(partition, key, key_type).await
            }

            async fn store(
                self: Arc<Self>,
                partition: Partition,
                key: Hash,
                value: Hash,
                key_type: KeyType,
            ) -> Result<(), StoreError> {
                self.inner
                    .clone()
                    .store(partition, key, value, key_type)
                    .await
            }

            async fn compare_and_swap(
                self: Arc<Self>,
                partition: Partition,
                key: Hash,
                expected: Hash,
                value: Hash,
                key_type: KeyType,
            ) -> Result<Hash, StoreError> {
                self.swaps.fetch_add(1, Ordering::SeqCst);
                let rival = self.rival.lock().unwrap().take();
                if let Some(rival) = rival {
                    self.inner
                        .clone()
                        .compare_and_swap(partition, key, expected, rival, key_type)
                        .await?;
                }
                self.inner
                    .clone()
                    .compare_and_swap(partition, key, expected, value, key_type)
                    .await
            }

            async fn list(
                self: Arc<Self>,
                partition: Partition,
                key_type: KeyType,
            ) -> Result<KeyValueStream, StoreError> {
                self.inner.clone().list(partition, key_type).await
            }

            async fn flush(self: Arc<Self>, sync_data: bool) -> Result<(), StoreError> {
                self.inner.clone().flush(sync_data).await
            }
        }

        /// A repository over a [`WatchedStore`], with a branch whose head is revision 1.
        struct Fixture {
            repository: Arc<RepositoryContext>,
            store: Arc<dyn lore_storage::ImmutableStore>,
            watched: Arc<WatchedStore>,
            branch: BranchId,
            head: Hash,
        }

        async fn repository_at_revision_one()
        -> (Fixture, Arc<lore_revision::interface::ExecutionContext>) {
            let (immutable_store, mutable_store, execution) =
                test_store_create().await.expect("Failed to create stores");
            let watched = Arc::new(WatchedStore {
                inner: mutable_store,
                swaps: AtomicUsize::new(0),
                rival: std::sync::Mutex::new(None),
            });
            let repository = Arc::new(RepositoryContext::new_server_context(
                immutable_store.clone(),
                watched.clone(),
                random::<RepositoryId>(),
            ));
            let (branch, head) = LORE_CONTEXT
                .scope(execution.clone(), {
                    let repository = repository.clone();
                    async move {
                        let branch = create_test_branch(&repository).await;
                        let head =
                            push_linear_revisions(&repository, branch, Hash::default(), 1..=1)
                                .await[0];
                        (branch, head)
                    }
                })
                .await;
            watched.forget_swaps();
            (
                Fixture {
                    repository,
                    store: immutable_store,
                    watched,
                    branch,
                    head,
                },
                execution,
            )
        }

        /// A revision on `parent` that this client never made: what another writer
        /// pushes. Its metadata names another author, so it is not one of the
        /// backlog's revisions by coincidence of bytes.
        async fn their_revision(
            repository: &Arc<RepositoryContext>,
            branch: BranchId,
            parent: Hash,
            number: u64,
        ) -> Arc<State> {
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata.set_branch(branch).expect("set branch");
            metadata
                .set_string("author", "someone else")
                .expect("set author");
            let metadata_hash = metadata
                .serialize(repository.clone())
                .await
                .expect("serialize metadata");
            let state = Arc::new(State::new());
            state.set_parent_self(parent);
            state.set_revision_number(number);
            state.set_metadata_hash(metadata_hash);
            state
                .serialize(repository.clone(), &get_write_token())
                .await
                .expect("serialize state");
            state
        }

        /// Revisions `numbers`, each the first parent of the next, starting on
        /// `parent`. Saved and never pushed, as a backlog is.
        async fn backlog(
            repository: &Arc<RepositoryContext>,
            branch: BranchId,
            parent: Hash,
            numbers: std::ops::RangeInclusive<u64>,
        ) -> Vec<Arc<State>> {
            let mut parent = parent;
            let mut revisions = Vec::new();
            for number in numbers {
                let state =
                    serialize_revision(repository, branch, parent, Hash::default(), number).await;
                parent = state.revision();
                revisions.push(state);
            }
            revisions
        }

        /// What an unforced push of `tip` answers, as a client pushing a backlog asks it.
        async fn push_tip(
            repository: &Arc<RepositoryContext>,
            branch: BranchId,
            tip: Hash,
        ) -> Result<PushResult, Status> {
            push(
                repository.clone(),
                branch,
                tip,
                true,
                false,
                false,
                DEFAULT_HISTORY_STEP_SIZE,
                RevisionListAcceleration::default(),
            )
            .await
        }

        async fn head_of(repository: &Arc<RepositoryContext>, branch: BranchId) -> Hash {
            load_latest(repository.clone(), branch)
                .await
                .expect("load the branch head")
        }

        #[tokio::test]
        async fn a_backlog_moves_the_head_once_to_its_tip() {
            let (
                Fixture {
                    repository,
                    watched,
                    branch,
                    head,
                    ..
                },
                execution,
            ) = repository_at_revision_one().await;

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let revisions = backlog(&repository, branch, head, 2..=4).await;
                let tip = revisions[2].revision();

                let result = push_tip(&repository, branch, tip)
                    .await
                    .expect("a backlog on the head's line is a fast-forward");

                assert!(result.success);
                assert!(!result.fast_forward_merged);
                assert_eq!(result.revision, tip);
                assert_eq!(result.revision_number, 4);
                assert_eq!(head_of(&repository, branch).await, tip);
                assert_eq!(
                    watched.swaps(),
                    1,
                    "three revisions, one compare-and-swap of the branch head"
                );
            }))
            .await;
        }

        /// The history-step index names the revision a push of one revision at a
        /// time would have left it naming: the highest at or below each boundary.
        ///
        /// One step from 98 to 105 would seal boundary 100 with revision 98, the
        /// older end of that one step. Revision 100 is in the backlog, so it is the
        /// answer.
        #[tokio::test]
        async fn a_backlog_seals_each_boundary_with_the_revision_that_owns_it() {
            let (
                Fixture {
                    repository, branch, ..
                },
                execution,
            ) = repository_at_revision_one().await;

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let head = head_of(&repository, branch).await;
                let line = push_linear_revisions(&repository, branch, head, 2..=98).await;
                let revisions = backlog(&repository, branch, line[96], 99..=105).await;

                let result = push_tip(&repository, branch, revisions[6].revision())
                    .await
                    .expect("push the backlog");
                assert!(result.success);

                assert_eq!(
                    load_step_key(&repository, branch, 100).await,
                    Some(revisions[1].revision()),
                    "boundary 100 belongs to revision 100"
                );
                assert_eq!(load_step_key(&repository, branch, 200).await, None);
            }))
            .await;
        }

        /// A head that is not on the tip's first-parent line is answered as it
        /// always was: not a fast-forward, naming the head, which stays put.
        #[tokio::test]
        async fn a_head_off_the_tips_line_is_still_not_a_fast_forward() {
            let (
                Fixture {
                    repository,
                    watched,
                    branch,
                    head,
                    ..
                },
                execution,
            ) = repository_at_revision_one().await;

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let theirs = their_revision(&repository, branch, head, 2)
                    .await
                    .revision();
                push(
                    repository.clone(),
                    branch,
                    theirs,
                    true,
                    false,
                    false,
                    DEFAULT_HISTORY_STEP_SIZE,
                    RevisionListAcceleration::default(),
                )
                .await
                .expect("their revision is pushed");
                watched.forget_swaps();
                let revisions = backlog(&repository, branch, head, 2..=4).await;

                let result = push_tip(&repository, branch, revisions[2].revision())
                    .await
                    .expect("a diverged push is answered, not failed");

                assert!(!result.success);
                assert_eq!(result.revision, theirs);
                assert_eq!(result.revision_number, 0);
                assert_eq!(head_of(&repository, branch).await, theirs);
                assert_eq!(watched.swaps(), 0, "nothing was swapped");
            }))
            .await;
        }

        /// Every step is verified, not only the tip: a revision below the tip whose
        /// tree is missing refuses the whole move and moves nothing.
        #[tokio::test]
        async fn a_backlog_missing_a_fragment_below_its_tip_moves_nothing() {
            let (
                Fixture {
                    repository,
                    store,
                    watched,
                    branch,
                    head,
                },
                execution,
            ) = repository_at_revision_one().await;
            let (peer_store, peer_mutable, _) =
                test_store_create().await.expect("Failed to create stores");

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let peer = Arc::new(RepositoryContext::new_server_context(
                    peer_store.clone(),
                    peer_mutable,
                    repository.id,
                ));

                // Revision 2 holds a file, and only its own block and its metadata
                // reach this store.
                let with_a_file = serialize_revision_with_a_file(&peer, branch).await;
                with_a_file.set_parent_self(head);
                with_a_file.set_revision_number(2);
                let second = with_a_file
                    .serialize(peer.clone(), &get_write_token())
                    .await
                    .expect("serialize revision 2");
                for hash in [second, with_a_file.metadata_hash()] {
                    hand_over_fragment(&peer_store, &store, repository.id, hash).await;
                }
                let tip = backlog(&repository, branch, second, 3..=3).await[0].revision();

                let Err(status) = push_tip(&repository, branch, tip).await else {
                    panic!("a backlog missing a revision's tree cannot move the head");
                };

                assert_eq!(status.code(), Code::FailedPrecondition);
                assert!(
                    status
                        .message()
                        .starts_with("Failed to collect new fragments"),
                    "{}",
                    status.message()
                );
                assert_eq!(head_of(&repository, branch).await, head);
                assert_eq!(watched.swaps(), 0, "nothing was swapped");
            }))
            .await;
        }

        /// A wrong revision number below the tip cannot be corrected without
        /// changing every revision above it, so the move is not taken. Pushed one
        /// revision at a time, each is corrected as it goes.
        #[tokio::test]
        async fn a_misnumbered_revision_below_the_tip_is_not_a_fast_forward() {
            let (
                Fixture {
                    repository,
                    watched,
                    branch,
                    head,
                    ..
                },
                execution,
            ) = repository_at_revision_one().await;

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let misnumbered =
                    serialize_revision(&repository, branch, head, Hash::default(), 7).await;
                let tip = serialize_revision(
                    &repository,
                    branch,
                    misnumbered.revision(),
                    Hash::default(),
                    8,
                )
                .await;

                let result = push_tip(&repository, branch, tip.revision())
                    .await
                    .expect("answered, not failed");

                assert!(!result.success);
                assert_eq!(result.revision, head);
                assert_eq!(head_of(&repository, branch).await, head);
                assert_eq!(watched.swaps(), 0, "nothing was swapped");
            }))
            .await;
        }

        /// [`lore_revision::branch::push::MAX_PUSH_CHAIN`] revisions move in one step. One more is answered
        /// as not a fast-forward with the head unchanged, which is what tells a
        /// client to move in shorter steps or one revision at a time.
        #[tokio::test]
        async fn the_longest_backlog_moves_in_one_step_and_one_more_does_not() {
            let (
                Fixture {
                    repository,
                    watched,
                    branch,
                    head,
                    ..
                },
                execution,
            ) = repository_at_revision_one().await;

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let longest = lore_revision::branch::push::MAX_PUSH_CHAIN as u64;
                let revisions = backlog(&repository, branch, head, 2..=longest + 2).await;

                let too_long =
                    push_tip(&repository, branch, revisions[longest as usize].revision())
                        .await
                        .expect("answered, not failed");
                assert!(!too_long.success);
                assert_eq!(head_of(&repository, branch).await, head);
                assert_eq!(watched.swaps(), 0);

                let tip = revisions[longest as usize - 1].revision();
                let longest_move = push_tip(&repository, branch, tip)
                    .await
                    .expect("the longest backlog moves");
                assert!(longest_move.success);
                assert_eq!(longest_move.revision_number, longest + 1);
                assert_eq!(head_of(&repository, branch).await, tip);
                assert_eq!(watched.swaps(), 1);
            }))
            .await;
        }

        /// Another writer moving the head between this push's checks and its swap
        /// wins: the push is told not a fast-forward, naming the winner, and the
        /// winner's head is what the branch keeps. A lost race, never a lost update.
        #[tokio::test]
        async fn a_head_that_moves_during_the_move_is_a_lost_race() {
            let (
                Fixture {
                    repository,
                    watched,
                    branch,
                    head,
                    ..
                },
                execution,
            ) = repository_at_revision_one().await;

            Box::pin(LORE_CONTEXT.scope(execution, async move {
                let theirs = their_revision(&repository, branch, head, 2).await;
                let revisions = backlog(&repository, branch, head, 2..=4).await;
                watched.race_with(theirs.revision());

                let result = push_tip(&repository, branch, revisions[2].revision())
                    .await
                    .expect("a lost race is answered, not failed");

                assert!(!result.success);
                assert_eq!(result.revision, theirs.revision());
                assert_eq!(
                    head_of(&repository, branch).await,
                    theirs.revision(),
                    "the head the other writer set stays"
                );
                assert_eq!(watched.swaps(), 1, "one swap, which lost");
            }))
            .await;
        }
    }
}
