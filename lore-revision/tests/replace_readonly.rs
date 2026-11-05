// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
#[cfg(test)]
mod tests {
    #![allow(clippy::disallowed_methods)] // Test fixture writes; not subject to repository write-token discipline.

    use std::path::Path;

    use lore_base::runtime::LORE_CONTEXT;
    use lore_base::runtime::runtime;
    use lore_revision::commit;
    use lore_revision::commit::CommitOptions;
    use lore_revision::file;
    use lore_revision::file::reset;
    use lore_revision::file::reset::ResetOptions;
    use lore_revision::interface::LoreArray;
    use lore_revision::interface::LoreString;
    use lore_revision::lore::RepositoryId;
    use lore_revision::stage::StageOptions;

    include!("helper.rs");

    fn set_readonly(path: &Path, readonly: bool) {
        let mut permissions = std::fs::metadata(path)
            .unwrap_or_else(|_| panic!("Failed to stat {}", path.display()))
            .permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        permissions.set_readonly(readonly);
        std::fs::set_permissions(path, permissions)
            .unwrap_or_else(|_| panic!("Failed to set permissions on {}", path.display()));
    }

    /// A reset puts the committed bytes back over working files another tool marked read-only: a
    /// Perforce leftover, or an editor plugin's file nobody has locked. Both of Lore's write
    /// routes are covered. Content under the fragment threshold is written in place, and larger
    /// content is staged beside the file and renamed over it; on Windows each failed with access
    /// denied before Lore cleared the attribute.
    #[tokio::test]
    async fn reset_replaces_a_read_only_working_file() {
        let (immutable_store, mutable_store, execution) =
            test_store_create().await.expect("Failed to create stores");
        let repository_id = RepositoryId::from(uuid::Uuid::now_v7());

        #[allow(clippy::disallowed_methods)]
        runtime()
            .spawn(LORE_CONTEXT.scope(execution.clone(), async move {
                let test =
                    test_repository_create(immutable_store, mutable_store, repository_id).await;
                let path = test.path.clone();
                let small = path.join("small.uasset");
                let large = path.join("large.uasset");
                let committed_small = b"committed small asset".to_vec();
                // Over `FRAGMENT_SIZE_THRESHOLD` (256 KiB), so it is fragmented and restored
                // through a temporary file renamed over the target.
                let committed_large: Vec<u8> = (0..(1024 * 1024 + 17))
                    .map(|i: u32| (i % 251) as u8)
                    .collect();
                test_file_write(&small, &committed_small);
                test_file_write(&large, &committed_large);

                file::stage::stage(
                    test.repository.clone(),
                    &test.write_token,
                    LoreArray::from_vec(vec![LoreString::from(&path)]),
                    StageOptions {
                        scan: true,
                        ..StageOptions::default()
                    },
                )
                .await
                .expect("Failed to stage");
                let options = CommitOptions {
                    message: String::new(),
                    link_messages: std::collections::HashMap::new(),
                    link: None,
                    layer_messages: std::collections::HashMap::new(),
                    layer: None,
                };
                Box::pin(commit::commit(
                    test.repository.clone(),
                    &test.write_token,
                    options,
                ))
                .await
                .expect("Failed to commit");

                test_file_write(&small, b"edited locally, then made read-only");
                test_file_write(&large, &vec![7u8; 300 * 1024]);
                set_readonly(&small, true);
                set_readonly(&large, true);

                let result = reset::reset(
                    test.repository.clone(),
                    &test.write_token,
                    LoreArray::from_vec(vec![LoreString::from(&path)]),
                    LoreString::default(),
                    ResetOptions::default(),
                )
                .await;

                let small_now = std::fs::read(&small).expect("Failed to read small.uasset");
                let large_now = std::fs::read(&large).expect("Failed to read large.uasset");
                let small_readonly = std::fs::metadata(&small)
                    .expect("Failed to stat small.uasset")
                    .permissions()
                    .readonly();
                let large_readonly = std::fs::metadata(&large)
                    .expect("Failed to stat large.uasset")
                    .permissions()
                    .readonly();
                // Leave the fixture removable whatever the outcome.
                set_readonly(&small, false);
                set_readonly(&large, false);

                assert!(
                    result.is_ok(),
                    "reset over read-only files failed: {:?}",
                    result.err()
                );
                assert!(
                    small_now == committed_small,
                    "small.uasset still holds the local edit ({} bytes)",
                    small_now.len()
                );
                assert!(
                    large_now == committed_large,
                    "large.uasset still holds the local edit ({} bytes)",
                    large_now.len()
                );
                // Lore leaves the replaced file writable. Putting an attribute back is the
                // client's policy, not Lore's.
                assert!(!small_readonly, "small.uasset is still read-only");
                assert!(!large_readonly, "large.uasset is still read-only");
            }))
            .await
            .expect("Test task failed");
    }
}
