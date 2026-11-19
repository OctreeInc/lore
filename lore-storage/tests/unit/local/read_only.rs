// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! A store opened read-only, on disk and through a read-only view of one file holding it.

use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use lore_base::test_util::TempDir;
use lore_storage::Address;
use lore_storage::Context;
use lore_storage::Fragment;
use lore_storage::FragmentFlags;
use lore_storage::Partition;
use lore_storage::hash;
use lore_storage::immutable_store::ImmutableStore;
use lore_storage::local::immutable_store::ImmutableStoreSettings;
use lore_storage::local::immutable_store::LocalImmutableStore;

const PARTITION: [u8; 16] = [0x5e; 16];

/// A store under `dir` holding `count` payloads of a few kilobytes each, flushed and closed.
async fn written_store(dir: &Path, count: u8) -> Vec<(Address, Bytes)> {
    let store: Arc<dyn ImmutableStore> =
        LocalImmutableStore::new(Some(dir.to_path_buf()), ImmutableStoreSettings::default())
            .await
            .expect("create store");
    let mut written = Vec::new();
    for seed in 0..count {
        let payload = Bytes::from(
            (0..4096 + seed as usize * 97)
                .map(|index| (index as u8).wrapping_mul(seed | 1))
                .collect::<Vec<u8>>(),
        );
        let address = Address {
            hash: hash::hash_slice(payload.as_ref()),
            context: Context::from([seed; 16]),
        };
        store
            .clone()
            .put(
                Partition::from(PARTITION),
                address,
                Fragment {
                    flags: FragmentFlags::PayloadStoredLocal.bits(),
                    size_payload: payload.len() as u32,
                    size_content: payload.len() as u64,
                },
                Some(payload.clone()),
                false,
            )
            .await
            .expect("put");
        written.push((address, payload));
    }
    store.clone().flush(lore_storage::FlushOptions::SYNC_DATA).await.expect("flush");
    drop(store);
    written
}

/// Every file under `dir`, one after another in a single file at `blob`, as the members of a view.
fn pack_into_one_file(dir: &Path, blob: &Path) -> Vec<lore_io::view::ViewFile> {
    let mut bytes = b"a few bytes ahead of the first member".to_vec();
    let mut members = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("list") {
            let path = entry.expect("entry").path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
            let content = std::fs::read(&path).expect("read member");
            members.push(lore_io::view::ViewFile {
                path: path.strip_prefix(dir).expect("under dir").to_path_buf(),
                offset: bytes.len() as u64,
                len: content.len() as u64,
            });
            bytes.extend_from_slice(&content);
            bytes.extend_from_slice(b"--");
        }
    }
    std::fs::write(blob, bytes).expect("write blob");
    members
}

fn read_only() -> ImmutableStoreSettings {
    ImmutableStoreSettings {
        read_only: true,
        ..Default::default()
    }
}

async fn assert_reads(store: &Arc<dyn ImmutableStore>, written: &[(Address, Bytes)]) {
    for (address, payload) in written {
        let got = store
            .clone()
            .get(Partition::from(PARTITION), *address)
            .await
            .expect("get");
        assert_eq!(got.payload.as_ref(), Some(payload), "payload reads back");
    }
}

async fn assert_refuses_writes(store: &Arc<dyn ImmutableStore>) {
    let payload = Bytes::from_static(b"never written");
    let refused = store
        .clone()
        .put(
            Partition::from(PARTITION),
            Address {
                hash: hash::hash_slice(payload.as_ref()),
                context: Context::from([0xff; 16]),
            },
            Fragment {
                flags: FragmentFlags::PayloadStoredLocal.bits(),
                size_payload: payload.len() as u32,
                size_content: payload.len() as u64,
            },
            Some(payload),
            false,
        )
        .await;
    assert!(refused.is_err(), "a read-only store takes no put");
    store
        .clone()
        .flush(lore_storage::FlushOptions::SYNC_DATA)
        .await
        .expect("a flush has nothing to write");
}

/// The tree under `dir`, path and length of each file, to show nothing changed.
fn listing(dir: &Path) -> Vec<(PathBuf, u64)> {
    let mut found = Vec::new();
    let mut pending = vec![dir.to_path_buf()];
    while let Some(next) = pending.pop() {
        for entry in std::fs::read_dir(&next).expect("list") {
            let entry = entry.expect("entry");
            let path = entry.path();
            if path.is_dir() {
                pending.push(path.clone());
            }
            found.push((path, entry.metadata().expect("stat").len()));
        }
    }
    found.sort();
    found
}

#[tokio::test]
async fn a_store_opened_read_only_reads_everything_and_changes_nothing() {
    let dir = TempDir::new("lore-storage-read-only-");
    let written = written_store(dir.as_ref(), 24).await;
    let before = listing(dir.as_ref());

    let first: Arc<dyn ImmutableStore> =
        LocalImmutableStore::new(Some(dir.as_ref().to_path_buf()), read_only())
            .await
            .expect("open read-only");
    // No flock: a second reader opens beside the first.
    let second: Arc<dyn ImmutableStore> =
        LocalImmutableStore::new(Some(dir.as_ref().to_path_buf()), read_only())
            .await
            .expect("open read-only again");
    assert_reads(&first, &written).await;
    assert_reads(&second, &written).await;
    assert_refuses_writes(&first).await;
    drop((first, second));

    assert_eq!(listing(dir.as_ref()), before, "nothing on disk changed");
}

#[tokio::test]
async fn a_store_reads_in_place_from_a_view_of_one_file_holding_it() {
    let dir = TempDir::new("lore-storage-view-");
    let written_dir = dir.child("written");
    let written = written_store(&written_dir, 24).await;
    let blob = dir.child("archive");
    let members = pack_into_one_file(&written_dir, &blob);
    std::fs::remove_dir_all(&written_dir).expect("only the one file is left");

    let root = dir.child("view");
    let _mounted = lore_io::view::mount(&root, &blob, members).expect("mount");
    let store: Arc<dyn ImmutableStore> = LocalImmutableStore::new(Some(root.clone()), read_only())
        .await
        .expect("open through the view");
    assert_reads(&store, &written).await;
    assert_refuses_writes(&store).await;
    drop(store);
    assert!(
        !root.exists(),
        "nothing was written where the view is mounted"
    );
}

#[tokio::test]
async fn a_store_through_a_view_does_not_open_for_writing() {
    let dir = TempDir::new("lore-storage-view-writable-");
    let written_dir = dir.child("written");
    written_store(&written_dir, 2).await;
    let blob = dir.child("archive");
    let members = pack_into_one_file(&written_dir, &blob);
    let root = dir.child("view");
    let _mounted = lore_io::view::mount(&root, &blob, members).expect("mount");
    assert!(
        LocalImmutableStore::new(Some(root.clone()), ImmutableStoreSettings::default())
            .await
            .is_err(),
        "a store that would take a flock and write is refused a read-only view"
    );
    assert!(!root.exists());
}
