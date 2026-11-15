// SPDX-FileCopyrightText: 2026 Epic Games, Inc.
// SPDX-License-Identifier: MIT
//! Read-only views: a directory tree whose files are byte ranges of one file.
//!
//! An uncompressed archive (a stored ZIP, say) holds each of its members as one contiguous range
//! of its bytes. Mounting a view of it at a path makes every [`IoDriver`] call under that path
//! answer from the archive, so a reader built on the driver (a store opened read-only, for one)
//! reads the members in place, without unpacking a copy of them.
//!
//! A view is read-only and fixed when it is mounted: its source must not change while it is
//! mounted. Opening a member for writing, creating, renaming or removing anything under the root
//! fails with [`std::io::ErrorKind::ReadOnlyFilesystem`]. Paths are matched lexically, component
//! by component, against the root as it was mounted, so a caller builds them from that root.
//!
//! [`IoDriver`]: crate::IoDriver

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::ffi::OsString;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::RwLock;

use crate::DirEntry;
use crate::Stat;
use crate::StatKind;

/// One member of a view: where it sits under the root, and the range of the source holding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ViewFile {
    /// Relative to the view's root, of normal components only.
    pub path: PathBuf,
    /// Where the member's bytes start in the source.
    pub offset: u64,
    /// How many bytes it has.
    pub len: u64,
}

/// A mounted view.
#[derive(Debug)]
pub(crate) struct View {
    root: PathBuf,
    source: PathBuf,
    files: BTreeMap<PathBuf, (u64, u64)>,
    dirs: BTreeSet<PathBuf>,
}

/// What a path under a view names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Found<'a> {
    File {
        source: &'a Path,
        offset: u64,
        len: u64,
    },
    Directory,
    Missing,
}

impl View {
    pub(crate) fn find(&self, relative: &Path) -> Found<'_> {
        if let Some(&(offset, len)) = self.files.get(relative) {
            return Found::File {
                source: &self.source,
                offset,
                len,
            };
        }
        if self.dirs.contains(relative) {
            return Found::Directory;
        }
        Found::Missing
    }

    /// The children of the directory `relative`, as a listing describes them.
    pub(crate) fn list(&self, relative: &Path) -> Vec<std::io::Result<DirEntry>> {
        let mut names: BTreeMap<OsString, Stat> = BTreeMap::new();
        let child_of = |path: &Path| -> Option<OsString> {
            let rest = path.strip_prefix(relative).ok()?;
            let mut components = rest.components();
            let first = components.next()?;
            components
                .next()
                .is_none()
                .then(|| first.as_os_str().to_owned())
        };
        for dir in &self.dirs {
            if let Some(name) = child_of(dir) {
                names.insert(name, dir_stat());
            }
        }
        for (file, &(_, len)) in &self.files {
            if let Some(name) = child_of(file) {
                names.insert(name, file_stat(len));
            }
        }
        names
            .into_iter()
            .map(|(file_name, stat)| {
                Ok(DirEntry {
                    file_name,
                    metadata: Some(stat),
                    is_symlink: false,
                })
            })
            .collect()
    }
}

pub(crate) fn dir_stat() -> Stat {
    Stat {
        kind: StatKind::Directory,
        len: 0,
        modified: None,
        mode: None,
    }
}

pub(crate) fn file_stat(len: u64) -> Stat {
    Stat {
        kind: StatKind::File,
        len,
        modified: None,
        mode: None,
    }
}

fn mounts() -> &'static RwLock<Vec<Arc<View>>> {
    static MOUNTS: RwLock<Vec<Arc<View>>> = RwLock::new(Vec::new());
    &MOUNTS
}

/// The view `path` falls under, and the path relative to its root.
pub(crate) fn lookup(path: &Path) -> Option<(Arc<View>, PathBuf)> {
    let mounts = mounts()
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if mounts.is_empty() {
        return None;
    }
    mounts.iter().find_map(|view| {
        path.strip_prefix(&view.root)
            .ok()
            .map(|relative| (Arc::clone(view), relative.to_path_buf()))
    })
}

/// Whether `path` falls under a mounted view.
pub fn is_viewed(path: &Path) -> bool {
    lookup(path).is_some()
}

/// Whether `path` names something: a view's file or directory under a mounted root, and
/// otherwise what [`Path::exists`] says.
pub fn exists(path: &Path) -> bool {
    match lookup(path) {
        Some((view, relative)) => view.find(&relative) != Found::Missing,
        None => path.exists(),
    }
}

/// The children of `path` with whether each is a directory, for a path under a mounted view, or
/// `None` for any other path, which the caller lists as it would have.
///
/// # Errors
///
/// `NotFound` for a path the view does not hold, and `NotADirectory` for one of its files.
pub fn list(path: &Path) -> Option<std::io::Result<Vec<(OsString, bool)>>> {
    let (view, relative) = lookup(path)?;
    Some(match view.find(&relative) {
        Found::Directory => Ok(view
            .list(&relative)
            .into_iter()
            .filter_map(Result::ok)
            .map(|entry| {
                let is_dir = entry
                    .metadata
                    .is_some_and(|stat| stat.kind == StatKind::Directory);
                (entry.file_name, is_dir)
            })
            .collect()),
        Found::File { .. } => Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!("{} is not a directory", path.display()),
        )),
        Found::Missing => Err(not_found(path)),
    })
}

pub(crate) fn read_only(path: &Path) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::ReadOnlyFilesystem,
        format!("{} is in a read-only view", path.display()),
    )
}

pub(crate) fn no_metadata(path: &Path) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!(
            "{} is in a read-only view, which has no metadata to give: ask for its stat",
            path.display()
        ),
    )
}

pub(crate) fn not_found(path: &Path) -> std::io::Error {
    std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("{} is not in its view", path.display()),
    )
}

/// A mounted view, unmounted when dropped.
#[derive(Debug)]
pub struct Mounted {
    view: Arc<View>,
}

impl Mounted {
    pub fn root(&self) -> &Path {
        &self.view.root
    }

    pub fn source(&self) -> &Path {
        &self.view.source
    }
}

impl Drop for Mounted {
    fn drop(&mut self) {
        let mut mounts = mounts()
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        mounts.retain(|view| !Arc::ptr_eq(view, &self.view));
    }
}

fn is_plain(path: &Path) -> bool {
    path.components().next().is_some()
        && path
            .components()
            .all(|component| matches!(component, Component::Normal(_)))
}

fn invalid(message: String) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message)
}

/// Mounts a read-only view at `root` of `files`, each a range of `source`.
///
/// `root` must be absolute, and must neither contain nor fall under another mounted root. Each
/// member's path must be relative and plain, no two members may share a path, and a member may
/// not sit under another member. The ranges are not checked against `source`: a range past its
/// end reads short, as a file cut short would.
///
/// # Errors
///
/// `InvalidInput` for a root or member that breaks the rules above, and `AlreadyExists` for a
/// root that overlaps a mounted one.
pub fn mount(
    root: impl Into<PathBuf>,
    source: impl Into<PathBuf>,
    files: impl IntoIterator<Item = ViewFile>,
) -> std::io::Result<Mounted> {
    let root = root.into();
    let source = source.into();
    if !root.is_absolute() {
        return Err(invalid(format!(
            "view root {} is not absolute",
            root.display()
        )));
    }
    let mut members = BTreeMap::new();
    let mut dirs = BTreeSet::new();
    dirs.insert(PathBuf::new());
    for file in files {
        if !is_plain(&file.path) {
            return Err(invalid(format!(
                "view member {} is not a plain relative path",
                file.path.display()
            )));
        }
        let mut parent = file.path.parent();
        while let Some(dir) = parent {
            dirs.insert(dir.to_path_buf());
            parent = dir.parent();
        }
        if members
            .insert(file.path.clone(), (file.offset, file.len))
            .is_some()
        {
            return Err(invalid(format!(
                "view member {} appears twice",
                file.path.display()
            )));
        }
    }
    if let Some(clash) = members.keys().find(|path| dirs.contains(*path)) {
        return Err(invalid(format!(
            "view member {} is also a directory",
            clash.display()
        )));
    }
    let view = Arc::new(View {
        root,
        source,
        files: members,
        dirs,
    });
    let mut mounts = mounts()
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(other) = mounts
        .iter()
        .find(|other| other.root.starts_with(&view.root) || view.root.starts_with(&other.root))
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "view root {} overlaps the mounted {}",
                view.root.display(),
                other.root.display()
            ),
        ));
    }
    mounts.push(Arc::clone(&view));
    Ok(Mounted { view })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn root(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("lore-io-view-{name}-{}", std::process::id()))
    }

    fn member(path: &str, offset: u64, len: u64) -> ViewFile {
        ViewFile {
            path: PathBuf::from(path),
            offset,
            len,
        }
    }

    #[test]
    fn a_view_names_its_files_and_their_directories() {
        let root = root("names");
        let mounted = mount(
            &root,
            "/archive.zip",
            [member("a/b/one", 10, 3), member("a/two", 20, 4)],
        )
        .unwrap();
        assert!(exists(&root));
        assert!(exists(&root.join("a/b")));
        assert!(exists(&root.join("a/b/one")));
        assert!(!exists(&root.join("a/three")));
        let (view, relative) = lookup(&root.join("a")).unwrap();
        let names: Vec<_> = view
            .list(&relative)
            .into_iter()
            .map(|entry| entry.unwrap())
            .map(|entry| (entry.file_name, entry.metadata.unwrap().kind))
            .collect();
        assert_eq!(
            names,
            vec![
                (OsString::from("b"), StatKind::Directory),
                (OsString::from("two"), StatKind::File)
            ]
        );
        let listed = list(&root.join("a")).unwrap().unwrap();
        assert_eq!(
            listed,
            vec![(OsString::from("b"), true), (OsString::from("two"), false)]
        );
        assert!(list(&root.join("a/two")).unwrap().is_err());
        assert!(list(Path::new("/not/viewed")).is_none());
        drop(mounted);
        assert!(!is_viewed(&root.join("a")));
    }

    #[test]
    fn a_view_refuses_what_it_cannot_hold() {
        let root = root("refuses");
        assert!(mount("relative", "/x", []).is_err());
        assert!(mount(&root, "/x", [member("../up", 0, 1)]).is_err());
        assert!(mount(&root, "/x", [member("a", 0, 1), member("a", 1, 1)]).is_err());
        assert!(mount(&root, "/x", [member("a", 0, 1), member("a/b", 1, 1)]).is_err());
        let _held = mount(&root, "/x", [member("a", 0, 1)]).unwrap();
        let clash = mount(root.join("inner"), "/y", []).unwrap_err();
        assert_eq!(clash.kind(), std::io::ErrorKind::AlreadyExists);
    }
}
