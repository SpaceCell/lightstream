# Apache Arrow Flatbuffers Files

This folder contains the **Flatbuffers** files from the **Apache Arrow** project, used under **Apache Licensing**.

These are used to generate the schemas required to implement the **Apache Arrow IPC Format**, as 
per the official specification [here](https://arrow.apache.org/docs/format/Columnar.html#serialisation-and-interprocess-communication-ipc).

`Schema.fbs` and `Message.fbs` are reduced to the types Minarrow supports. Union
members carry the tag values from the official Arrow files (e.g. `LargeUtf8 = 20`),
because Flatbuffers assigns union tags positionally and the wire format must match
other Arrow implementations. When adding a type, copy its table and tag value from
the official file.

To regenerate the bindings in `src/arrow/`:

```
flatc --rust --gen-all -o <outdir> flatb/Message.fbs
flatc --rust --gen-all -o <outdir> flatb/File.fbs
flatc --rust --gen-all -o <outdir> flatb/Schema.fbs
```

then prepend the licence header and `#![allow(warnings)]` to each file and place
them at `src/arrow/{message,file,schema}.rs`.

Lightstream is not affiliated with *Apache Arrow* or the *Apache Software Foundation (ASF)*.
See `../THIRD_PARTY_LICENSES` for further details.
