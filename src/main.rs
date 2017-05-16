//    Copyright 2017 Vector Creations Limited
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.


// extern crate base64;
// extern crate blake2_rfc;
#[macro_use]
extern crate clap;
extern crate humansize;
extern crate linear_map;
extern crate rusqlite;
extern crate twox_hash;
extern crate walkdir;


// use blake2_rfc::blake2b::Blake2b;
use humansize::{FileSize, file_size_opts as options};
use clap::{App, Arg};
use linear_map::LinearMap;
use std::io;
use std::io::Read;
use std::fs::File;
use std::hash::Hasher;
use std::collections::BTreeMap;
use std::path::PathBuf;
use walkdir::WalkDir;


fn copy<R: io::Read, W: Hasher>(reader: &mut R, writer: &mut W) -> io::Result<u64> {
    let mut buf = [0; 64 * 1024];
    let mut written = 0;
    loop {
        let len = match reader.read(&mut buf) {
            Ok(0) => return Ok(written),
            Ok(len) => len,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e),
        };
        writer.write(&buf[..len]);
        written += len as u64;
    }
}


fn to_hash(path: &PathBuf) -> u64 {
    let mut file = File::open(path).unwrap();
    let mut hasher = twox_hash::XxHash::default();
    copy(&mut file, &mut hasher).unwrap();
    hasher.finish()
}


fn read_file(path: &PathBuf) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    let mut vec = Vec::new();
    file.read_to_end(&mut vec).unwrap();
    vec
}


fn partition_by<I, F, R>(paths: I, f: F) -> LinearMap<R, Vec<PathBuf>>
    where I: Iterator<Item=PathBuf>, F: Fn(&PathBuf) -> R, R: Eq
{
    let mut map = LinearMap::with_capacity(paths.size_hint().0);
    for path in paths {
        let key = f(&path);
        map.entry(key).or_insert_with(Vec::new).push(path);
    }
    map
}


const DB_TABLE_SCHEMA: &'static str = r#"
CREATE TABLE files (
    hash BIGINT NOT NULL,
    path TEXT NOT NULL,
    size BIGINT NOT NULL
);
"#;


fn main() {
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .author(crate_authors!("\n"))
        .arg(Arg::with_name("media_directory")
            .help("The location of the media store")
            .index(1)
            .required(true))
        .get_matches();

    let path = matches.value_of("media_directory").unwrap();

    println!("Creating DB...");

    let db = rusqlite::Connection::open_in_memory().expect("failed to open sqlite db");
    db.execute_batch(DB_TABLE_SCHEMA).expect("failed to create db schema");

    let mut paths_by_size = BTreeMap::new();
    let mut total_files = 0;

    println!("Walking fs...");

    for entry in WalkDir::new(path) {
        let entry = entry.unwrap();
        if entry.file_type().is_dir() {
            continue
        }

        let file_size = entry.metadata().unwrap().len() as usize;
        paths_by_size.entry(file_size).or_insert_with(Vec::new).push(entry.path().to_owned());

        total_files += 1;
        if total_files % 10000 == 0 {
            println!("Handled {} files", total_files);
        }
    }

    println!("Handled {} files", total_files);
    println!();

    let mut total_wasted_size = 0;

    for (file_size, paths) in paths_by_size {
        if paths.len() == 1 {
            continue
        }

        let by_hash = partition_by(paths.into_iter(), to_hash);

        for (hash, paths) in by_hash {
            if paths.len() == 1 {
                continue
            }

            let by_contents = partition_by(paths.into_iter(), read_file);

            for (_, paths) in by_contents {
                if paths.len() == 1 {
                    continue
                }

                // print!("Duplicate {} paths (hash: {})\n", paths.len(), hash);
                for path in &paths {
                    db.execute("INSERT INTO files (hash, path, size) VALUES (?, ?, ?)", &[&(hash as i64), &path.to_str().unwrap(), &(file_size as i64)]).expect("failed to write to db");
                    // print!("  {}\n", path.display());
                }

                let wasted = file_size * (paths.len() - 1);
                print!(
                    " Size: {}. Wasting {}.\n",
                    file_size.file_size(options::CONVENTIONAL).unwrap(),
                    wasted.file_size(options::CONVENTIONAL).unwrap(),
                );

                total_wasted_size += wasted;
            }
        }
    }

    println!();
    println!("Total wasted size: {}", total_wasted_size.file_size(options::CONVENTIONAL).unwrap());

    let mut disk_db = rusqlite::Connection::open("media_store_sizes.db").expect("failed to open sqlite db");
    let backup = rusqlite::backup::Backup::new(&db, &mut disk_db).expect("failed to create backup");
    backup.run_to_completion(5, std::time::Duration::from_millis(0), None).expect("failed to write to disk");
}
