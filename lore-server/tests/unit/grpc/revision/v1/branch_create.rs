// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
mod input_length_validation {
    use lore_revision::branch;
    use lore_revision::repository;
    use lore_server::grpc::revision::v1::branch_create::*;

    #[test]
    fn accepts_valid_input() {
        validate_create_input("my-branch", "feature", "alice").expect("valid input should pass");
    }

    #[test]
    fn accepts_name_at_max_length() {
        let name = "a".repeat(branch::MAX_NAME_LEN);
        validate_create_input(&name, "feature", "alice")
            .expect("name at exactly MAX_NAME_LEN should pass");
    }

    #[test]
    fn rejects_oversized_branch_name() {
        let long_name = "a".repeat(branch::MAX_NAME_LEN + 1);
        let err = validate_create_input(&long_name, "feature", "alice")
            .expect_err("should reject oversized name");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Branch name exceeds maximum length"));
    }

    #[test]
    fn rejects_oversized_category() {
        let long_cat = "a".repeat(branch::MAX_NAME_LEN + 1);
        let err = validate_create_input("my-branch", &long_cat, "alice")
            .expect_err("should reject oversized category");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message()
                .contains("Branch category exceeds maximum length")
        );
    }

    #[test]
    fn rejects_oversized_creator() {
        let long_creator = "a".repeat(repository::MAX_NAME_LEN + 1);
        let err = validate_create_input("my-branch", "feature", &long_creator)
            .expect_err("should reject oversized creator");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("Creator exceeds maximum length"));
    }
}

use std::sync::Arc;

use lore_base::runtime::LORE_CONTEXT;
use lore_proto::lore::revision::v1::BranchCreateRequest;
use lore_proto::lore::revision::v1::BranchCreateResponse;
use lore_revision::branch;
use lore_revision::lore::BranchId;
use lore_revision::lore::RepositoryId;
use lore_revision::repository::RepositoryContext;
use lore_revision::util;
use lore_server::auth::jwt::AuthorizationToken;
use lore_server::grpc::revision::v1::branch_create::*;
use lore_server::hooks::HookDispatcher;
use lore_telemetry::InstrumentProvider;
use lore_transport::grpc::REPOSITORY_ID_KEY;
use opentelemetry::KeyValue;
use rand::random;
use tonic::Request;

use super::hook_recorder::Seen;
use super::hook_recorder::recording_dispatcher;
use crate::notification::testing::MockNotificationSender;
use crate::store::test_support::test_store_create;

struct TestInstrumentProvider {}

impl InstrumentProvider for TestInstrumentProvider {
    fn namespace(&self) -> &'static str {
        "test"
    }
    fn labels(&self) -> &[KeyValue] {
        &[]
    }
}

mod direct_handling {
    use super::*;

    #[tokio::test]
    async fn create_returns_full_branch_record() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let mut notification_sender = MockNotificationSender::new();
        notification_sender
            .expect_branch_created()
            .return_once(|_, _| ());
        let notification_sender = Arc::new(notification_sender);
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let mut request = Request::new(BranchCreateRequest {
                id: branch_id.into(),
                name: "main".into(),
                creator: Some("alice".into()),
                category: "default".into(),
                stack: vec![],
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let hook_dispatcher = HookDispatcher::empty();
            let response = handler(
                request,
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("Request failed");

            let branch = response
                .into_inner()
                .branch
                .expect("response should include Branch");
            assert_eq!(branch.name, "main");
            assert_eq!(branch.creator, "alice");
            assert_eq!(branch.category, "default");
            assert!(!branch.deleted);
            assert!(branch.created > 0);
            assert!(!branch.id.is_empty());
            assert!(!branch.metadata.is_empty());
        }))
        .await;
    }

    #[tokio::test]
    async fn create_stamps_created_in_milliseconds() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let mut notification_sender = MockNotificationSender::new();
        notification_sender
            .expect_branch_created()
            .return_once(|_, _| ());
        let notification_sender = Arc::new(notification_sender);
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let mut request = Request::new(BranchCreateRequest {
                id: branch_id.into(),
                name: "main".into(),
                creator: Some("alice".into()),
                category: "default".into(),
                stack: vec![],
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let hook_dispatcher = HookDispatcher::empty();
            let before = util::time::timestamp();
            let response = handler(
                request,
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("Request failed");
            let after = util::time::timestamp();

            let created = response
                .into_inner()
                .branch
                .expect("response should include Branch")
                .created;
            assert!(
                (before..=after).contains(&created),
                "created {created} outside [{before}, {after}], expected epoch milliseconds"
            );
        }))
        .await;
    }

    #[tokio::test]
    async fn empty_name_returns_invalid_argument() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let notification_sender = Arc::new(MockNotificationSender::new());
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let mut request = Request::new(BranchCreateRequest {
                id: branch_id.into(),
                name: String::new(),
                creator: Some("alice".into()),
                category: "default".into(),
                stack: vec![],
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );

            let hook_dispatcher = HookDispatcher::empty();
            let err = handler(
                request,
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect_err("empty name should fail");
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
        }))
        .await;
    }

    #[tokio::test]
    async fn unset_creator_falls_back_to_jwt_identity() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let mut notification_sender = MockNotificationSender::new();
        notification_sender
            .expect_branch_created()
            .return_once(|_, _| ());
        let notification_sender = Arc::new(notification_sender);
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let mut request = Request::new(BranchCreateRequest {
                id: branch_id.into(),
                name: "main".into(),
                creator: None,
                category: "default".into(),
                stack: vec![],
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
            );
            request.extensions_mut().insert(AuthorizationToken {
                user_id: "jwt-user".into(),
                ..AuthorizationToken::default()
            });

            let hook_dispatcher = HookDispatcher::empty();
            let response = handler(
                request,
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("Request failed");

            let branch = response
                .into_inner()
                .branch
                .expect("response should include Branch");
            assert_eq!(branch.creator, "jwt-user");
        }))
        .await;
    }

    #[tokio::test]
    async fn duplicate_id_returns_already_exists() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let mut notification_sender = MockNotificationSender::new();
        notification_sender
            .expect_branch_created()
            .return_once(|_, _| ());
        let notification_sender = Arc::new(notification_sender);
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let make_request = || {
                let mut request = Request::new(BranchCreateRequest {
                    id: branch_id.into(),
                    name: "main".into(),
                    creator: Some("alice".into()),
                    category: "default".into(),
                    stack: vec![],
                });
                request.metadata_mut().insert_bin(
                    REPOSITORY_ID_KEY,
                    tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
                );
                request
            };

            let hook_dispatcher = HookDispatcher::empty();
            handler(
                make_request(),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("first create should succeed");

            let err = handler(
                make_request(),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect_err("duplicate id should fail");
            assert_eq!(err.code(), tonic::Code::AlreadyExists);
        }))
        .await;
    }

    fn create_request(repository: RepositoryId, name: &str) -> Request<BranchCreateRequest> {
        let mut request = Request::new(BranchCreateRequest {
            id: BranchId::from(uuid::Uuid::now_v7()).into(),
            name: name.into(),
            creator: Some("alice".into()),
            category: "default".into(),
            stack: vec![],
        });
        request.metadata_mut().insert_bin(
            REPOSITORY_ID_KEY,
            tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
        );
        request
    }

    #[tokio::test]
    async fn pre_hook_sees_branch_name_and_service_account() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let mut notification_sender = MockNotificationSender::new();
        notification_sender
            .expect_branch_created()
            .return_once(|_, _| ());
        let notification_sender = Arc::new(notification_sender);
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let mut request = create_request(repository, "release/1.0");
            request.extensions_mut().insert(AuthorizationToken {
                user_id: "ci-bot".into(),
                is_service_account: Some(true),
                ..AuthorizationToken::default()
            });

            let (hook_dispatcher, seen) = recording_dispatcher(false);
            handler(
                request,
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("create should succeed");

            assert_eq!(
                *seen.lock(),
                vec![Seen {
                    branch_name: Some("release/1.0".into()),
                    is_service_account: Some("true".into()),
                }]
            );
        }))
        .await;
    }

    #[tokio::test]
    async fn rejected_create_creates_nothing() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let notification_sender = Arc::new(MockNotificationSender::new());
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let (hook_dispatcher, _seen) = recording_dispatcher(true);
            let err = handler(
                create_request(repository, "main"),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect_err("a rejected create should fail");

            assert_eq!(err.code(), tonic::Code::PermissionDenied);
            let repository_context = Arc::new(RepositoryContext::new_server_context(
                immutable_store.clone(),
                mutable_store.clone(),
                repository,
            ));
            assert!(
                branch::load_name_to_id_local(repository_context, "main")
                    .await
                    .is_err(),
                "a rejected create must not map the name",
            );
        }))
        .await;
    }

    #[tokio::test]
    async fn oversized_name_is_rejected_before_pre_hooks() {
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");

        let notification_sender = Arc::new(MockNotificationSender::new());
        let instrument_provider = TestInstrumentProvider {};

        Box::pin(LORE_CONTEXT.scope(execution.clone(), async move {
            let (hook_dispatcher, seen) = recording_dispatcher(false);
            let name = "x".repeat(branch::MAX_NAME_LEN + 1);
            let err = handler(
                create_request(repository, &name),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store.clone(),
                mutable_store.clone(),
                notification_sender.clone(),
                &None, /* no forwarded requests */
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect_err("an oversized name should fail");

            assert_eq!(err.code(), tonic::Code::InvalidArgument);
            assert!(seen.lock().is_empty());
        }))
        .await;
    }
}

mod forwarded_request {
    use std::sync::Mutex;

    use async_trait::async_trait;
    use lore_server::grpc::forwarded_requests::ForwardedRequestResult;
    use lore_server::grpc::forwarded_requests::ForwardedRequests;
    use lore_server::grpc::forwarded_requests::InternalClientError;
    use lore_server::grpc::forwarded_requests::RpcFlags;
    use lore_server::grpc::forwarded_requests::revision_service::ForwardedRevisionServiceClient;
    use tonic::Response;
    use tonic::Status;

    use super::*;

    /// Single-use client that returns a pre-configured result on its one call.
    struct SingleShotClient {
        response: Arc<Mutex<Option<ForwardedRequestResult<BranchCreateResponse>>>>,
    }

    #[async_trait]
    impl ForwardedRevisionServiceClient for SingleShotClient {
        async fn branch_create(
            &mut self,
            _request: Request<BranchCreateRequest>,
        ) -> ForwardedRequestResult<BranchCreateResponse> {
            self.response
                .lock()
                .unwrap()
                .take()
                .expect("branch_create called more than once")
        }

        async fn branch_delete(
            &mut self,
            _request: Request<lore_proto::lore::revision::v1::BranchDeleteRequest>,
        ) -> ForwardedRequestResult<lore_proto::lore::revision::v1::BranchDeleteResponse> {
            unreachable!("branch_delete should not be called in branch_create tests")
        }

        async fn branch_get(
            &mut self,
            _request: Request<lore_proto::lore::revision::v1::BranchGetRequest>,
        ) -> ForwardedRequestResult<lore_proto::lore::revision::v1::BranchGetResponse> {
            unreachable!("branch_get should not be called in branch_create tests")
        }

        async fn branch_list(
            &mut self,
            _request: Request<lore_proto::lore::revision::v1::BranchListRequest>,
        ) -> ForwardedRequestResult<lore_server::grpc::revision::v1::branch_list::BranchListStream>
        {
            unreachable!("branch_list should not be called in branch_create tests")
        }
    }

    struct StubForwardedRequests {
        flags: RpcFlags,
        response: Arc<Mutex<Option<ForwardedRequestResult<BranchCreateResponse>>>>,
    }

    impl StubForwardedRequests {
        fn forwarding_enabled(response: ForwardedRequestResult<BranchCreateResponse>) -> Arc<Self> {
            Arc::new(Self {
                flags: RpcFlags {
                    revision_branch_create: true,
                    ..Default::default()
                },
                response: Arc::new(Mutex::new(Some(response))),
            })
        }

        fn forwarding_disabled(
            response: ForwardedRequestResult<BranchCreateResponse>,
        ) -> Arc<Self> {
            Arc::new(Self {
                flags: RpcFlags {
                    revision_branch_create: false,
                    ..Default::default()
                },
                response: Arc::new(Mutex::new(Some(response))),
            })
        }
    }

    impl ForwardedRequests for StubForwardedRequests {
        fn rpc_flags(&self) -> &RpcFlags {
            &self.flags
        }

        fn forwarded_revision_service(&self) -> Box<dyn ForwardedRevisionServiceClient> {
            Box::new(SingleShotClient {
                response: Arc::clone(&self.response),
            })
        }

        fn forwarded_repository_service(
            &self,
        ) -> Box<dyn lore_server::grpc::forwarded_requests::repository_service::ForwardedRepositoryServiceClient>
{
            unreachable!("forwarded_repository_service should not be called in branch_create tests")
        }

        fn forwarded_lock_service(
            &self,
        ) -> Box<dyn lore_server::grpc::forwarded_requests::lock_service::ForwardedLockServiceClient>
        {
            unreachable!("forwarded_lock_service should not be called in branch_create tests")
        }
    }

    fn make_request(repository: RepositoryId, branch_id: BranchId) -> Request<BranchCreateRequest> {
        let mut request = Request::new(BranchCreateRequest {
            id: branch_id.into(),
            name: "main".into(),
            creator: Some("alice".into()),
            category: "default".into(),
            stack: vec![],
        });
        request.metadata_mut().insert_bin(
            REPOSITORY_ID_KEY,
            tonic::metadata::BinaryMetadataValue::from_bytes(repository.data()),
        );
        request
    }

    #[tokio::test]
    async fn delegates_to_remote_and_returns_response() {
        // When the flag is enabled the other server's response is returned directly;
        // branch_create_implementation is NOT called so the local store stays empty.
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let notification_sender = Arc::new(MockNotificationSender::new()); // no branch_created expected
        let instrument_provider = TestInstrumentProvider {};

        let branch_request = lore_proto::lore::model::v1::Branch {
            name: "main".into(),
            creator: "alice".into(),
            category: "default".into(),
            ..Default::default()
        };
        let branch_response = Ok(Ok(Response::new(BranchCreateResponse {
            branch: Some(branch_request),
        })));
        let forwarded_requests = StubForwardedRequests::forwarding_enabled(branch_response);

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let hook_dispatcher = HookDispatcher::empty();

            let response = handler(
                make_request(repository, branch_id),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store,
                mutable_store,
                notification_sender,
                &Some(forwarded_requests as Arc<dyn ForwardedRequests>),
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("should succeed");

            let branch = response
                .into_inner()
                .branch
                .expect("response should include Branch");
            assert_eq!(branch.name, "main");
            assert_eq!(branch.creator, "alice");
        }))
        .await;
    }

    #[tokio::test]
    async fn error_status_returned_to_caller() {
        // An error status from the forwarded server is forwarded directly to the original caller.
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let notification_sender = Arc::new(MockNotificationSender::new());
        let instrument_provider = TestInstrumentProvider {};

        let forwarded_request_result = Ok(Err(Status::already_exists("test error forwarded")));
        let forwarded_requests =
            StubForwardedRequests::forwarding_enabled(forwarded_request_result);

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let hook_dispatcher = HookDispatcher::empty();

            let err = handler(
                make_request(repository, branch_id),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store,
                mutable_store,
                notification_sender,
                &Some(forwarded_requests as Arc<dyn ForwardedRequests>),
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect_err("forwarded error should propagate");

            assert_eq!(err.code(), tonic::Code::AlreadyExists);
            assert!(err.message().contains("test error forwarded"));
        }))
        .await;
    }

    #[tokio::test]
    async fn internal_client_error_maps_to_internal_status() {
        // A transport-level failure (InternalClientError) is mapped to Status::internal.
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let notification_sender = Arc::new(MockNotificationSender::new());
        let instrument_provider = TestInstrumentProvider {};

        let forwarded_requests =
            StubForwardedRequests::forwarding_enabled(Err(InternalClientError::internal("oops")));

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let hook_dispatcher = HookDispatcher::empty();

            let err = handler(
                make_request(repository, branch_id),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store,
                mutable_store,
                notification_sender,
                &Some(forwarded_requests as Arc<dyn ForwardedRequests>),
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect_err("transport error should become internal status");

            assert_eq!(err.code(), tonic::Code::Internal);
            assert!(err.message().contains("Error making forwarded request"));
        }))
        .await;
    }

    #[tokio::test]
    async fn flag_disabled_falls_through_to_local_execution() {
        // When revision_branch_create is false the local path runs, even if a
        // ForwardedRequests is present. The stub client is not called.
        let repository = random::<RepositoryId>();
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let mut notification_sender = MockNotificationSender::new();
        notification_sender
            .expect_branch_created()
            .return_once(|_, _| ());
        let notification_sender = Arc::new(notification_sender);
        let instrument_provider = TestInstrumentProvider {};

        // response is irrelevant — client must never be called
        let forwarded_result = Ok(Err(Status::internal("should not be called")));
        let forwarded_requests = StubForwardedRequests::forwarding_disabled(forwarded_result);

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let branch_id = BranchId::from(uuid::Uuid::now_v7());
            let hook_dispatcher = HookDispatcher::empty();

            let response = handler(
                make_request(repository, branch_id),
                &crate::util::test_support::allow_all_authorizer(),
                immutable_store,
                mutable_store,
                notification_sender,
                &Some(forwarded_requests as Arc<dyn ForwardedRequests>),
                &hook_dispatcher,
                &instrument_provider,
            )
            .await
            .expect("local execution should succeed");

            let branch = response
                .into_inner()
                .branch
                .expect("response should include Branch");
            assert_eq!(branch.name, "main");
        }))
        .await;
    }
}

/// A create that stopped after the branch's record and before its head — a
/// server stopped between the two writes — leaves a branch whose first push
/// the client cannot complete: the push reads the missing head as the zero
/// revision and answers `NotFound`, and the client's create of the branch
/// finds the record. A retry of that same create finishes it.
mod create_stopped_before_its_head {
    use lore_base::types::BranchPoint;
    use lore_base::types::Hash;
    use lore_revision::branch::DEFAULT_HISTORY_STEP_SIZE;
    use lore_revision::state::State;
    use lore_server::grpc::get_write_token;
    use lore_server::grpc::handlers::branch_push::PushHandler;
    use lore_server::grpc::handlers::branch_push::PushReport;
    use lore_server::grpc::handlers::branch_push::push;
    use lore_server::grpc::server::RevisionListAcceleration;
    use mockall::predicate::eq;
    use tonic::Code;
    use tonic::Status;

    use super::*;
    use crate::store::test_support::StopsOnceStore;

    const NAME: &str = "warmer barrel";

    type Stops = fn(Arc<dyn lore_storage::MutableStore>) -> Arc<dyn lore_storage::MutableStore>;

    struct Server {
        id: RepositoryId,
        immutable_store: Arc<dyn lore_storage::ImmutableStore>,
        mutable_store: Arc<dyn lore_storage::MutableStore>,
        repository: Arc<RepositoryContext>,
        notification_sender: Arc<MockNotificationSender>,
    }

    impl Server {
        /// A server that stops once, where `stops` says, and which expects to
        /// announce each branch of `created` as created the given number of
        /// times.
        fn new(
            immutable_store: Arc<dyn lore_storage::ImmutableStore>,
            mutable_store: Arc<dyn lore_storage::MutableStore>,
            stops: Stops,
            created: &[(BranchId, usize)],
        ) -> Self {
            let id = random::<RepositoryId>();
            let mutable_store = stops(mutable_store);
            let mut notification_sender = MockNotificationSender::new();
            for (branch, times) in created {
                notification_sender
                    .expect_branch_created()
                    .with(eq(id), eq(*branch))
                    .times(*times)
                    .return_const(());
            }
            let repository = Arc::new(RepositoryContext::new_server_context(
                immutable_store.clone(),
                mutable_store.clone(),
                id,
            ));
            Self {
                id,
                immutable_store,
                mutable_store,
                repository,
                notification_sender: Arc::new(notification_sender),
            }
        }

        /// A revision on `branch` whose parent is `parent`.
        async fn revision(&self, branch: BranchId, parent: Hash, number: u64) -> Hash {
            let mut metadata = lore_revision::metadata::Metadata::new();
            metadata.set_branch(branch).expect("set branch");
            let metadata_hash = metadata
                .serialize(self.repository.clone())
                .await
                .expect("serialize metadata");
            let state = State::new();
            state.set_parent_self(parent);
            state.set_revision_number(number);
            state.set_metadata_hash(metadata_hash);
            state
                .serialize(self.repository.clone(), &get_write_token())
                .await
                .expect("serialize state")
        }

        /// The create a client sends: its own id for the branch, the name, and
        /// where the branch was made from.
        async fn create(
            &self,
            branch: BranchId,
            stack: &[BranchPoint],
        ) -> Result<lore_proto::lore::model::v1::Branch, Status> {
            let mut request = Request::new(BranchCreateRequest {
                id: branch.into(),
                name: NAME.into(),
                creator: Some("alice".into()),
                category: "default".into(),
                stack: stack.iter().map(Into::into).collect(),
            });
            request.metadata_mut().insert_bin(
                REPOSITORY_ID_KEY,
                tonic::metadata::BinaryMetadataValue::from_bytes(self.id.data()),
            );
            let response = handler(
                request,
                &crate::util::test_support::allow_all_authorizer(),
                self.immutable_store.clone(),
                self.mutable_store.clone(),
                self.notification_sender.clone(),
                &None, /* no forwarded requests */
                &HookDispatcher::empty(),
                &TestInstrumentProvider {},
            )
            .await?;
            Ok(response
                .into_inner()
                .branch
                .expect("response should include Branch"))
        }

        async fn push(&self, branch: BranchId, revision: Hash) -> Result<Hash, Status> {
            push(
                self.repository.clone(),
                branch,
                revision,
                crate::util::test_support::PUSH_ANY,
                false,
                false,
                DEFAULT_HISTORY_STEP_SIZE,
                RevisionListAcceleration::default(),
                &mut PushReport::new(PushHandler::V1, Default::default()),
            )
            .await
            .map(|pushed| pushed.revision)
        }

        async fn head(&self, branch: BranchId) -> Option<Hash> {
            branch::load_latest_local(self.repository.clone(), branch)
                .await
                .ok()
        }

        /// Creates `branch` from `stack` and checks the create stopped after
        /// the record and before the head.
        async fn create_and_stop_before_the_head(&self, branch: BranchId, stack: &[BranchPoint]) {
            self.create(branch, stack)
                .await
                .expect_err("the create stops before its head");
            assert!(
                branch::metadata_local(self.repository.clone(), branch)
                    .await
                    .is_ok(),
                "the branch's record was written"
            );
            assert_eq!(
                branch::load_name_to_id_local(self.repository.clone(), NAME)
                    .await
                    .ok(),
                Some(branch),
                "the name maps to the branch"
            );
            assert_eq!(self.head(branch).await, None, "the head was not written");
        }
    }

    #[tokio::test]
    async fn the_clients_retry_finishes_the_create_and_pushes() {
        let feature = BranchId::from(uuid::Uuid::now_v7());
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let server = Server::new(
            immutable_store,
            mutable_store,
            StopsOnceStore::before_the_head,
            &[(feature, 1)],
        );

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let main = BranchId::from(uuid::Uuid::now_v7());
            let point = server.revision(main, Hash::default(), 1).await;
            let saved = server.revision(feature, point, 2).await;
            let stack = [BranchPoint {
                branch: main,
                revision: point,
            }];

            // The branch's first push: the server does not know it, so the
            // client creates it, and the create stops before its head.
            let first = server.push(feature, saved).await.expect_err("unknown");
            assert_eq!(first.code(), Code::NotFound, "{first:?}");
            server
                .create_and_stop_before_the_head(feature, &stack)
                .await;

            // The client's retry: a push, then the create, then the push.
            let refused = server
                .push(feature, saved)
                .await
                .expect_err("a branch with no head refuses the push");
            assert_eq!(refused.code(), Code::NotFound, "{refused:?}");
            let created = server
                .create(feature, &stack)
                .await
                .expect("the same create, retried, finishes the branch");
            assert_eq!(BranchId::from(created.id.clone()), feature);
            assert_eq!(Hash::from(created.latest.clone()), point);
            assert_eq!(
                server
                    .push(feature, saved)
                    .await
                    .expect("the retry's push goes through"),
                saved
            );

            // The branch is whole now, so creating it again is refused and
            // leaves its head where the push put it.
            let again = server
                .create(feature, &stack)
                .await
                .expect_err("the branch exists");
            assert_eq!(again.code(), Code::AlreadyExists, "{again:?}");
            assert_eq!(server.head(feature).await, Some(saved));
        }))
        .await;
    }

    #[tokio::test]
    async fn only_the_same_create_finishes_it() {
        let feature = BranchId::from(uuid::Uuid::now_v7());
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let server = Server::new(
            immutable_store,
            mutable_store,
            StopsOnceStore::before_the_head,
            &[(feature, 1)],
        );

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let main = BranchId::from(uuid::Uuid::now_v7());
            let point = server.revision(main, Hash::default(), 1).await;
            let later = server.revision(main, point, 2).await;
            let stack = [BranchPoint {
                branch: main,
                revision: point,
            }];
            server
                .create_and_stop_before_the_head(feature, &stack)
                .await;

            // Another branch of the same name.
            let other = BranchId::from(uuid::Uuid::now_v7());
            let taken = server
                .create(other, &stack)
                .await
                .expect_err("the name is taken");
            assert_eq!(taken.code(), Code::AlreadyExists, "{taken:?}");
            assert_eq!(server.head(other).await, None);
            assert_eq!(server.head(feature).await, None);

            // The same branch, made from somewhere else.
            let elsewhere = [BranchPoint {
                branch: main,
                revision: later,
            }];
            let moved = server
                .create(feature, &elsewhere)
                .await
                .expect_err("not the create that stopped");
            assert_eq!(moved.code(), Code::AlreadyExists, "{moved:?}");
            assert_eq!(server.head(feature).await, None);

            // The create that stopped.
            server
                .create(feature, &stack)
                .await
                .expect("the same create, retried, finishes the branch");
            assert_eq!(server.head(feature).await, Some(point));
        }))
        .await;
    }

    /// A create can also stop between the branch's metadata and its name. The
    /// retry's push maps the name back to the branch, as it does for a deleted
    /// branch, and is refused for the missing head; the create then finds a
    /// branch with a record and no head, and finishes it.
    #[tokio::test]
    async fn a_create_stopped_before_its_name_is_finished_by_the_clients_retry() {
        let feature = BranchId::from(uuid::Uuid::now_v7());
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let server = Server::new(
            immutable_store,
            mutable_store,
            StopsOnceStore::before_the_name,
            &[(feature, 1)],
        );

        Box::pin(LORE_CONTEXT.scope(execution, async move {
            let main = BranchId::from(uuid::Uuid::now_v7());
            let point = server.revision(main, Hash::default(), 1).await;
            let saved = server.revision(feature, point, 2).await;
            let stack = [BranchPoint {
                branch: main,
                revision: point,
            }];

            let first = server.push(feature, saved).await.expect_err("unknown");
            assert_eq!(first.code(), Code::NotFound, "{first:?}");
            server
                .create(feature, &stack)
                .await
                .expect_err("the create stops before its name");
            assert!(
                branch::metadata_local(server.repository.clone(), feature)
                    .await
                    .is_ok(),
                "the branch's metadata was written"
            );
            assert!(
                branch::load_name_to_id_local(server.repository.clone(), NAME)
                    .await
                    .is_err(),
                "its name was not"
            );

            // The client's retry: a push, then the create, then the push.
            let refused = server.push(feature, saved).await.expect_err("no head");
            assert_eq!(refused.code(), Code::NotFound, "{refused:?}");
            assert_eq!(
                branch::load_name_to_id_local(server.repository.clone(), NAME)
                    .await
                    .ok(),
                Some(feature),
                "the push mapped the name back to the branch"
            );
            assert_eq!(server.head(feature).await, None);
            let created = server
                .create(feature, &stack)
                .await
                .expect("the same create, retried, finishes the branch");
            assert_eq!(Hash::from(created.latest.clone()), point);
            assert_eq!(
                server
                    .push(feature, saved)
                    .await
                    .expect("the retry's push goes through"),
                saved
            );
        }))
        .await;
    }
}
