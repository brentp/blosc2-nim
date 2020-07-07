import ./blosc2pkg/blosc2_sys
import strformat
import os

converter toCsize*(i:int): csize_t =
  i.csize_t
converter toCint*(i:int): cint =
  i.cint


proc buffer_info*(buffer: var seq[uint8]): tuple[uncompressed_bytes: int, compressed_bytes:int, blocksize:int, typesize:int, flags: int, complib:string] =
  ## given a compressed buffer report the compressed, uncompressed, and block-size
  var ub:csize_t
  var cb:csize_t
  var bs: csize_t
  buffer[0].addr.pointer.blosc_cbuffer_sizes(ub.addr, cb.addr, bs.addr)
  result.uncompressed_bytes = ub.int
  result.compressed_bytes = cb.int
  result.block_size = bs.int
  var ts:csize_t
  var flags:cint
  buffer[0].addr.pointer.blosc_cbuffer_metainfo(ts.addr, flags.addr)
  result.typesize = ts.int
  result.flags = flags.int
  result.complib = $buffer[0].addr.pointer.blosc_cbuffer_complib

proc create_cparams[T](codec:string, clevel:int=5, delta:bool=false, threads:int=4, use_dict:bool=false, schunk:pointer=nil): blosc2_cparams =
  var ctx = blosc2_cparams()
  ctx.compcode = blosc_compname_to_compcode(codec).uint8
  ctx.clevel = clevel.uint8
  ctx.typesize = sizeof(T)
  ctx.schunk = schunk
  ctx.nthreads = threads.int16
  ctx.filters = [0'u8, 0, 0, 0, 0, BloscFilters.SHUFFLE.uint8]
  if delta:
    ctx.filters[4] = BloscFilters.DELTA.uint8
  #ctx.filters_meta =  [0'u8, 0, 0, 0, 0, 0]
  ctx.prefilter = nil
  ctx.pparams = nil
  return ctx

proc compressContext*[T](codec:string, clevel:int=5, delta:bool=false, threads:int=4, use_dict:bool=false, schunk:pointer=nil): blosc2_context =
  var ctx = create_cparams[T](codec, clevel, delta, threads, use_dict, schunk)
  return blosc2_create_cctx(ctx)

proc decompressContext*(threads:int|int16=4, schunk:pointer=nil): blosc2_context =
  var ctx = blosc2_dparams()
  ctx.nthreads = threads.int16
  ctx.schunk = schunk
  return blosc2_create_dctx(ctx)

proc freeContext*(ctx:blosc2_context) =
  blosc2_free_ctx(ctx)

type superChunk*[T] = ref object
  c*: ptr blosc2_schunk

type Frame*[T] = ref object
  c*: ptr blosc2_frame
  schunk*: ptr blosc2_schunk
  buffer*: seq[T]

proc destroy_frame[T](f:Frame[T]) =
  if f.c != nil:
    discard f.c.blosc2_free_frame
  if f.schunk != nil:
    discard f.schunk.blosc2_free_schunk

proc destroy_chunk(c:superChunk) =
  if c.c != nil:
    discard c.c.blosc2_free_schunk

iterator metalayers*(f:Frame): tuple[key:string, value: seq[uint8]] =
  for i in 0..<f.schunk.nmetalayers:
    let key = f.schunk.metalayers[i].name
    let value = newSeq[uint8](f.schunk.metalayers[i].content_len)
    copyMem(value[0].unsafeAddr.pointer, f.schunk.metalayers[i].content.pointer, value.len)
    yield ($key, value)

proc add_metalayer*(f:Frame, key:string, value: seq[uint8]) =
  doAssert f.schunk.blosc2_add_metalayer(key, value[0].unsafeAddr, value.len.uint32) >= 0, "error adding metalayer"

proc metalayer*(f:Frame, key:string): seq[uint8] =
  # get the metalayer associated with the key
  var content: ptr uint8
  var content_len: uint32
  if f.schunk.blosc2_get_metalayer(key, content.addr, content_len.addr) < 0:
    raise newException(KeyError, "unknown metalayer: " & key)
  result.setLen(content_len)
  copyMem(result[0].addr.pointer, content.pointer, content_len.int)

proc has_metalayer*(f:Frame, key:string): bool =
  result = f.schunk.blosc2_has_metalayer(key.cstring) >= 0

proc add_metalayer*(f:Frame, key:string, value:string) =
  f.add_metalayer(key, cast[seq[uint8]](value))

proc newFrame*[T](path:string, mode:FileMode=fmWrite, codec:string="blosclz", clevel:int=5, delta:bool=false, threads:int=4, newChunk:bool=true): Frame[T] =
  new(result, destroy_frame[T])

  if mode == fmRead:
    doAssert path != "", "expected path with writable mode"
    result.c = blosc2_frame_from_file(path)
    result.schunk = blosc2_schunk_from_frame(result.c, false)
  elif mode == fmWrite:
    result.c = blosc2_new_frame(path)
    var cparams = create_cparams[T](codec, clevel, delta, threads, false, nil)
    var dparams = blosc2_dparams(nthreads:threads.int16, schunk:nil)
    result.schunk = blosc2_new_schunk(cparams, dparams, result.c)
  else:
    doAssert false, "NotImplemented: only fmRead and fmWrite are supported"

  if result.c == nil:
    raise newException(IOError, "blosc2: error opening file:" & path)

proc newSuperChunk*[T](codec:string="blosclz", clevel:int=5, delta:bool=false, threads:int=4, frame:Frame=nil): superChunk[T] =

  var cparams = create_cparams[T](codec, clevel, delta, threads, false, nil)
  var dparams = blosc2_dparams(nthreads:threads.int16, schunk:nil)
  if frame != nil:
    result.frame = frame
    result.c = blosc2_new_schunk(cparams, dparams, result.frame.c)
  else:
    result.c = blosc2_new_schunk(cparams, dparams, nil)

proc len*[T](s:superChunk[T]): int {.inline.} =
  ## number of chunks in the super chunk
  result = s.c.n_chunks

proc len*[T](f:Frame[T]): int {.inline.} =
  ## number of chunks in the super chunk
  result = f.schunk.n_chunks

proc add*[T](s:superChunk[T], input: var seq[T], newChunk:bool=false): int {.discardable.} =
  result = s.c.blosc2_schunk_append_buffer(input[0].addr.pointer, sizeof(T) * input.len)

proc add*[T](f:Frame[T], input: var seq[T], newChunk:bool=false): int {.discardable.} =
  result = f.schunk.blosc2_schunk_append_buffer(input[0].addr.pointer, sizeof(T) * input.len)

proc info*[T](f:Frame[T]): tuple[n_chunks: int32, uncompressed_bytes: int64, compressed_bytes: int64] =
  return (n_chunks: f.schunk.nchunks, uncompressed_bytes: f.schunk.nbytes,
          compressed_bytes: f.schunk.cbytes)

proc into*[T](s:ptr blosc2_schunk, i:int, output: var seq[T]) =
  if i < 0 or i > s.n_chunks: raise newException(IndexError, &"chunk {i} is out of bounds in superchunk with len: {s.n_chunks}")

  var chunk:ptr uint8
  var needs_free:bool
  let res = s.blosc2_schunk_get_chunk(i.cint, chunk.addr, needs_free.addr)
  if res < 0:
    raise newException(IndexDefect, "error:" & $res & " accessing chunk:" & $i)
  proc free(a1: pointer) {.cdecl, importc: "free", header: "<stdlib.h>".}

  var ub:csize_t
  var cb:csize_t
  var bs:csize_t
  chunk.pointer.blosc_cbuffer_sizes(ub.addr, cb.addr, bs.addr)
  if needs_free:
    free(chunk)

  output.setLen(int(ub.int / sizeof(T)))
  doAssert s.blosc2_schunk_decompress_chunk(i.cint, output[0].addr.pointer, ub) == ub.cint, "into: unexpected size of decompressed chunk"

proc `[]`*[T](f:var Frame[T], i:int): seq[T] =
  ## return the data associated with the i'th chunk. Note this uses a buffer to
  ## avoid extra copies so subsequent indexing will overwrite.
  f.schunk.into(i, f.buffer)
  shallow(f.buffer)
  return f.buffer

proc into*[T](s:superChunk[T], i:int, output: var seq[T]) =
  s.c.info(i, output)

proc into*[T](f:Frame[T], i:int, output: var seq[T]) =
  f.schunk.into(i, output)

proc compress*[T](ctx:blosc2_context, input:var seq[T], output: var seq[uint8], adjustOutputSize:bool=false) =
  if adjustOutputSize:
    output.setLen(input[0].sizeof * input.len + BLOSC_MAX_OVERHEAD)
  let size = ctx.blosc2_compress_ctx(input[0].sizeof * input.len, input[0].addr.pointer, output[0].addr.pointer, output[0].sizeof * output.len)
  if size <= 0:
    raise newException(IOError, "blosc2: error decompressing")
  output.setLen(size)

proc compress*[T](ctx:blosc2_context, input:var seq[T]): seq[uint8] =
  ctx.compress(input, result, true)

proc decompress*[T](ctx:blosc2_context, compressed:var seq[uint8], output: var seq[T]) =
  var bi = compressed.buffer_info
  output.setLen(int(bi.uncompressed_bytes / sizeof(T)))
  let size = ctx.blosc2_decompress_ctx(compressed[0].addr.pointer, output[0].addr.pointer, output[0].sizeof * output.len)
  if size != bi.uncompressed_bytes:
    raise newException(IOError, "blosc2: error decompressing")

when isMainModule:

  var x = newFrame[int32]("x.blc")
