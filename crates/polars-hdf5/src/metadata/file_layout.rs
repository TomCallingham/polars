use std::collections::HashMap;

use hdf5::{Dataset, File, Group, Result as Hdf5Result};

#[derive(Debug)]
struct DatasetMetadata {
    name: String,
    dimensions: Vec<usize>,
    data_type: String,
    attributes: HashMap<String, String>,
}

#[derive(Debug)]
struct GroupMetadata {
    name: String,
    groups: HashMap<String, GroupMetadata>,
    datasets: HashMap<String, DatasetMetadata>,
    attributes: HashMap<String, String>,
}
fn extract_dataset_metadata(dataset: &Dataset) -> Hdf5Result<DatasetMetadata> {
    let name = dataset.name();
    let dimensions = dataset.shape();
    let data_type = format!("{:?}", dataset.dtype()?);
    let mut attributes = HashMap::new();

    for attr in dataset.attr_names()? {
        let attribute = dataset.attr(&attr)?;
        // Assuming attributes are strings for simplicity; you may need to handle different types.
        let value: String = attribute.read()?;
        attributes.insert(attr, value);
    }

    Ok(DatasetMetadata {
        name,
        dimensions,
        data_type,
        attributes,
    })
}

fn extract_hdf5_layout(filename: str) -> Hdf5Result<GroupMetadata> {
    let file = File::open(filename)?;
    let name = group.name();
    let mut groups = HashMap::new();
    let mut datasets = HashMap::new();
    let mut attributes = HashMap::new();

    for attr in group.attr_names()? {
        let attribute = group.attr(&attr)?;
        let value: String = attribute.read()?;
        attributes.insert(attr, value);
    }

    for obj in group.member_names()? {
        let obj_path = format!("{}/{}", group.name(), obj);
        if let Ok(sub_group) = group.group(&obj) {
            let g_meta = extract_group_metadata(&sub_group)?;
            groups.insert(obj, g_meta);
        } else if let Ok(dataset) = group.dataset(&obj) {
            let d_meta = extract_dataset_metadata(&dataset)?;
            datasets.insert(obj, d_meta);
        }
    }

    Ok(GroupMetadata {
        name,
        groups,
        datasets,
        attributes,
    })
}
