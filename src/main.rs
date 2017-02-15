extern crate base64;
extern crate blake2_rfc;
extern crate humansize;
extern crate linear_map;
extern crate twox_hash;
extern crate walkdir;


// use blake2_rfc::blake2b::Blake2b;
use humansize::{FileSize, file_size_opts as options};
use linear_map::LinearMap;
use std::{env, io};
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


fn main() {
    let path = env::args().nth(1).expect("paramater");

    let mut paths_by_size = BTreeMap::new();

    let mut total_files = 0;

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

                print!("Duplicate {} paths (hash: {})\n", paths.len(), hash);
                for path in &paths {
                    print!("  {}\n", path.display());
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
}
