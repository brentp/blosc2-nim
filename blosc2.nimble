# Package

version       = "0.0.1"
author        = "Brent Pedersen"
description   = "blosc2 for nim"
license       = "MIT"


# Dependencies

requires "nim >= 0.19.9"
srcDir = "src"

skipDirs = @["tests"]

import os, strutils

task test, "run the tests":
  exec "nim c  -d:useSysAssert -d:useGcAssert --lineDir:on --debuginfo -r tests/test_blosc2"


