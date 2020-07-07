import blosc2

var values = newSeq[int32](100_000)
for i in 0..values.high:
  values[i] = int32(i * 3)

proc create() =
  var f = newFrame[int32]("x.blsc")

  f.add(values)
  assert f.len == 1 # 1 chunk
  f.add(values)
  assert f.len == 2 # 2 chunks
  f.add(values)
  assert f.len == 3 # 3 chunks

  echo f.info # (n_chunks: 3, uncompressed_bytes: 1200000, compressed_bytes: 14676)

proc use_frame() =

  var f = newFrame[int32]("x.blsc", mode=fmRead)

  # extract (and decompress) the first chunk.
  let extracted = f[0]

  assert extracted.len == values.len
  for i, e in extracted:
    assert e == values[i]

when isMainModule:
  create()

  use_frame()
