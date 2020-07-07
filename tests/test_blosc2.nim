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

    for delta in [true, false]:

      var f = newFrame[int32]("x.blc", delta=delta)
      #var si32 = newSuperChunk[int32](frame=f)
      #check si32.frame != nil

      # NOTE: now adding another chunk to the same frame.
      #

      var x = newSeq[int32](20000)
      var x2 = newSeq[int32](200)
      for i in 0..<x.len:
        x[i] = int32(i * 2)
      for i in 0..<x2.len:
        x2[i] = int32(i * 222)

      f.add(x)
      check f.schunk.n_chunks == 1

      f.add(x)
      check f.schunk.n_chunks == 2

      f.add(x2)
      check f.schunk.n_chunks == 3

      echo "nbytes:", f.schunk.nbytes
      echo "cbytes:", f.schunk.cbytes
      f = nil
      f = newFrame[int32]("x.blc", mode=fmRead)
      echo "nbytes:", f.schunk.nbytes
      echo "cbytes:", f.schunk.cbytes

      var output: seq[int32]
      f.into(0, output)
      for i, o in output:
        check o == x[i]

      f.into(2, output)
      check output.len == x2.len
      for i, o in output:
        check o == x2[i]


      var xx = f[2]

      xx[22] = 33'i32
      check f.buffer[22] == 33'i32

  test "frame with metalayer":
      var f = newFrame[int32]("x.blc")
      f.add_metalayer("info", "very important")
      f.add_metalayer("date", "also very important")

      for l in f.metalayers:
        echo l.key, ": ", cast[string](l.value)

      check f.has_metalayer("date")
      check f.has_metalayer("info")
      check not f.has_metalayer("absent")

      var val = f.metalayer("info")

      check cast[string](val) == "very important"
