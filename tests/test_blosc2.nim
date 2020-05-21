import unittest
import blosc2


suite "blosc2":
  test "that round-trip works":
    var x = newSeq[int64](1024)
    for i in 0..x.high:
      x[i] = 8'i32 * i.int64

    var ctx = compressContext("blosclz", typesize=4, delta=false)
    var dctx = decompressContext()

    var compressed = ctx.compress(x)
    check compressed.len < x.len * sizeof(x[0])

    var y = newSeq[int64](x.len)

    dctx.decompress(compressed, y)
    check y.len == x.len
    for i, yv in y:
      check yv == x[i]

