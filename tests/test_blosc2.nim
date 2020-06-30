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
    defer:
      ctx.freeContext
      dctx.freeContext

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
    defer:
      ctx.freeContext
      dctx.freeContext

    var compressed = ctx.compress(x)
    var got = newSeq[int32](4)

    ctx.getitem(compressed, 4, got)
    for i, g in got:
      check g == x[i + 4]

    check got.len == 4

  test "buffer info":

    var ctx = compressContext[int32]("lz4hc", delta=false)
    defer: ctx.freeContext
    var compressed = ctx.compress(x)

    var bi = compressed.buffer_info
    check bi.uncompressed_bytes == x.len * sizeof(x[0])
    check bi.typesize == sizeof(x[0])
    check bi.complib == "LZ4"
    echo bi


  test "schunk with frame":


    var f = newFrame("x.blc")
    var si32 = newSuperChunk[int32]() #frame=f)
    echo "TODO: handle this when frame is set"
    #check si32.frame != nil

    # NOTE: now adding another chunk to the same frame.
    var sf32 = newSuperChunk[float32](frame=f)
    check sf32.frame != nil
    var x = newSeq[int32](20000)
    for i in 0..<x.len:
      x[i] = int32(i * 2)
    si32.add(x)

    var f32 = newSeq[float32](20000)
    for i in 0..<f32.len:
      f32[i] = float32(i * 2)

    sf32.add(f32)

    check si32.len == 1


    var output: seq[int32]
    si32.into(0, output)

    for i, o in output:
      check o == x[i]
