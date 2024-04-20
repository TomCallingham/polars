use polars::prelude::*;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // --8<-- [start:read]
    let mut file = std::fs::File::open("docs/data/path.hdf5").unwrap();

    let df = hdf5Reader::new(&mut file).finish().unwrap();
    // --8<-- [end:read]
    println!("{}", df);

    // --8<-- [start:write]
    let mut df = df!(
        "foo" => &[1, 2, 3],
        "bar" => &[None, Some("bak"), Some("baz")],
    )
    .unwrap();

    let mut file = std::fs::File::create("docs/data/path.hdf5").unwrap();
    hdf5Writer::new(&mut file).finish(&mut df).unwrap();
    // --8<-- [end:write]

    // --8<-- [start:scan]
    let args = ScanArgshdf5::default();
    let lf = LazyFrame::scan_hdf5("./file.hdf5", args).unwrap();
    // --8<-- [end:scan]
    println!("{}", lf.collect()?);

    Ok(())
}
