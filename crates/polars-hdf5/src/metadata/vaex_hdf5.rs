use std::collections::HashMap;

// use hdf5::{Dataset, Datatype as Hdf5Datatype, File, Group, Object, Result as Hdf5Result};
use hdf5::{Datatype as Hdf5Datatype, File, Result as Hdf5Result};
// Group,
//
#[derive(Debug)]
pub struct Hdf5Metadata {
    pub file_path: String,
    // format: Hdf5Format,
    pub n_rows: usize,
    pub columns: Vec<String>,
    pub col_datatypes: HashMap<String, Hdf5Datatype>,
    pub col_path: HashMap<String, String>,
}

pub fn create_hdf5_schema_vaex(filename: &str) -> Hdf5Result<Hdf5Metadata> {
    println!("running vaex hdf5 layout");
    println!("filename: {}", filename);
    let mut root_group = File::open(filename)?.as_group()?;
    let vaex_table = "/table/columns";
    root_group = root_group.group(vaex_table)?;
    dbg!(&root_group);

    let mut columns: Vec<String> = Vec::new();

    let mut col_datatypes: HashMap<String, Hdf5Datatype> = HashMap::new();
    let mut col_path: HashMap<String, String> = HashMap::new();

    let mut n_rows: usize = 0;

    for obj in root_group.member_names()? {
        dbg!(&obj);
        let dataset = root_group.group(&obj)?.dataset("data")?;
        // let name = dataset.name();
        let name = obj.clone();
        columns.push(name.clone());
        let dimensions = dataset.shape();
        n_rows = dimensions[0];
        // let data_type = format!("{:?}", dataset.dtype()?);
        let data_type = dataset.dtype()?;
        col_datatypes.insert(name.clone(), data_type);
        let obj_path = format!("{}/{}", root_group.name(), obj);
        col_path.insert(name.clone(), obj_path);
    }
    let file_hdf5schema = Hdf5Metadata {
        file_path: filename.to_owned(),
        // format: Hdf5Format::Vaex,
        n_rows,
        columns,
        col_datatypes,
        col_path,
    };

    dbg!(&file_hdf5schema);

    Ok(file_hdf5schema)
}
