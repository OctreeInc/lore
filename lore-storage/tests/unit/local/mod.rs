// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT

mod fan_out;
mod immutable_store;
mod mutable_store;
mod read_only;
mod store_lock;

/// Run the flush `flush` starts, whose fan-out commit renames a `.new` file over the bucket file at
/// `blocked`, with a non-empty directory there, so that the commit fails at that rename once its
/// `level.pending` is on disk, and then remove the directory.
async fn fail_commit_at_rename<E, F: Future<Output = Result<(), E>>>(
    blocked: &std::path::Path,
    flush: impl FnOnce() -> F,
) {
    std::fs::create_dir_all(blocked).expect("block the bucket's rename");
    std::fs::write(blocked.join("blocker"), b"x").expect("block the bucket's rename");
    assert!(flush().await.is_err(), "the commit's rename fails");
    assert!(
        blocked
            .with_file_name(lore_storage::local::fan_out::LEVEL_PENDING_FILENAME)
            .exists(),
        "the commit failed before its level.pending"
    );
    std::fs::remove_dir_all(blocked).expect("unblock the bucket");
}
