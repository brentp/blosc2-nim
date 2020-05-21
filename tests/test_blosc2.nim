import unittest
import blosc2

var x = newSeq[int32](1024)
for i in 0..x.high:
  x[i] = 8'i32 * i.int32

suite "blosc2":
  test "that round-trip works":
    echo blosc_list_compressors()

    var ctx = compressContext[int32]("lz4hc", delta=false)
    var dctx = decompressContext()

    var compressed = ctx.compress(x)
    check compressed.len < x.len * sizeof(x[0])
    echo (x.len * sizeof(x[0])) / compressed.len

    var y = newSeq[int32](x.len)

    dctx.decompress(compressed, y)
    check y.len == x.len
    for i, yv in y:
      check yv == x[i]

  test "that getitem works":

    var ctx = compressContext[int32]("lz4hc", delta=false)
    var dctx = decompressContext()

    var compressed = ctx.compress(x)

    let got = getitem[int32](ctx, compressed, 4, 8)
    for i, g in got:
      check g == x[i + 4]

