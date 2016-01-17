extern crate zim;

use zim::{Zim, DirectoryEntry, Target};
use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use std::path::Path;


fn main() {


    let zim = Zim::new("wikispecies_en_all_2015-11.zim").ok().unwrap();
    let root_output = Path::new("zim_output_3");

    // map between cluster and directory entry
    let mut cluster_map = HashMap::new();

    println!("Building cluster map...");

    for i in zim.iterate_by_urls() {
        if let Some(Target::Cluster(cid, _)) = i.target {

            cluster_map.entry(cid).or_insert(Vec::new()).push(i);
        }
        //println!("{:?}", i);
        //if c > 10 { break; }
        //c += 1;
    }
    println!("Done!");


    
    // extract all non redirect entries
    let mut c = 0;
    for (cid, entries) in cluster_map {
        //println!("{}", cid);
        //println!("{:?}", entries);
        let cluster = zim.get_cluster(cid).unwrap();

        for entry in entries {
            if let Some(Target::Cluster(_cid, bid)) = entry.target {
                assert_eq!(cid, _cid);
                let mut s = String::new();
                s.push(entry.namespace);
                let out_path = root_output.join(&s).join(&entry.url);
                std::fs::create_dir_all(out_path.parent().unwrap());
                let data = cluster.get_blob(bid);
                let mut f = File::create(&out_path).unwrap();
                f.write_all(data);
                //println!("{} written to {}", entry.url, out_path.display());
            }
        }
        c += 1;
        println!("Finished processing cluster {} of {} ({}%)", c, zim.cluster_count, c * 100 / zim.cluster_count);
    }

    // link all redirects
    for entry in zim.iterate_by_urls() {
        // get redirect entry
        if let Some(Target::Redirect(redir)) = entry.target {
            let redir = zim.get_by_url_index(redir).unwrap();

            let mut s = String::new();
            s.push(redir.namespace);
            let src = root_output.join(&s).join(&redir.url);

            let mut d = String::new();
            d.push(entry.namespace);
            let dst = root_output.join(&s).join(&entry.url);
            
            if !dst.exists() {
                println!("{:?} -> {:?}", src, dst);
                std::fs::hard_link(src, dst).unwrap();
            }
        }
    }

    if let Some(main_page_idx) = zim.main_page_idx {
        let page = zim.get_by_url_index(main_page_idx).unwrap();
        println!("Main page is {}", page.url);
    }

}
