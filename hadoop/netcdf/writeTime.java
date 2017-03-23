  public void testWriteRecordOneAtaTime() throws IOException, InvalidRangeException {
    String filename = TestLocal.temporaryDataDir + "testWriteRecord2.nc";
    NetcdfFileWriter writer = NetcdfFileWriter.createNew(NetcdfFileWriter.Version.netcdf3, filename);

    // define dimensions, including unlimited
    Dimension latDim = writer.addDimension(null, "lat", 3);
    Dimension lonDim = writer.addDimension(null, "lon", 4);
    Dimension timeDim = writer.addUnlimitedDimension("time");

    // define Variables
    Variable lat = writer.addVariable(null, "lat", DataType.FLOAT, "lat");
    lat.addAttribute( new Attribute("units", "degrees_north"));
    Variable lon = writer.addVariable(null, "lon", DataType.FLOAT, "lon");
    lon.addAttribute( new Attribute("units", "degrees_east"));
    Variable rh = writer.addVariable(null, "rh", DataType.INT, "time lat lon");
    rh.addAttribute( new Attribute("long_name", "relative humidity"));
    rh.addAttribute( new Attribute("units", "percent"));
    Variable t = writer.addVariable(null, "T", DataType.DOUBLE, "time lat lon");
    t.addAttribute( new Attribute("long_name", "surface temperature"));
    t.addAttribute( new Attribute("units", "degC"));
    Variable time = writer.addVariable(null, "time", DataType.INT, "time");
    time.addAttribute( new Attribute("units", "hours since 1990-01-01"));

    // create the file
1)  writer.create();

    // write out the non-record variables
2)  writer.write(lat, Array.factory(new float[]{41, 40, 39}));
    writer.write(lon, Array.factory(new float[]{-109, -107, -105, -103}));

    //// heres where we write the record variables
    // different ways to create the data arrays.
    // Note the outer dimension has shape 1, since we will write one record at a time
3)  ArrayInt rhData = new ArrayInt.D3(1, latDim.getLength(), lonDim.getLength());
    ArrayDouble.D3 tempData = new ArrayDouble.D3(1, latDim.getLength(), lonDim.getLength());
    Array timeData = Array.factory(DataType.INT, new int[]{1});
    Index ima = rhData.getIndex();

    int[] origin = new int[]{0, 0, 0};
    int[] time_origin = new int[]{0};

    // loop over each record
4)  for (int timeIdx = 0; timeIdx < 10; timeIdx++) {
      // make up some data for this record, using different ways to fill the data arrays.
5.1)  timeData.setInt(timeData.getIndex(), timeIdx * 12);

      for (int latIdx = 0; latIdx < latDim.getLength(); latIdx++) {
        for (int lonIdx = 0; lonIdx < lonDim.getLength(); lonIdx++) {
5.2)      rhData.setInt(ima.set(0, latIdx, lonIdx), timeIdx * latIdx * lonIdx);
5.3)      tempData.set(0, latIdx, lonIdx, timeIdx * latIdx * lonIdx / 3.14159);
        }
      }
      // write the data out for one record
      // set the origin here
6)    time_origin[0] = timeIdx;
      origin[0] = timeIdx;
7)    writer.write(rh, origin, rhData);
      writer.write(t, origin, tempData);
      writer.write(time, time_origin, timeData);
    } // loop over record

    // all done
    writer.close();
  } 
