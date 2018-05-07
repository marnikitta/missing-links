package io.github.marnikitta.friends.generation

class FastRandom(seed: Long) {
  var u: Long = 0
  var v: Long = 4101842887655102017L
  var w: Long = 1

  {
    u = seed ^ v
    nextLong()
    v = u
    nextLong()
    w = v
    nextLong()
  }

  def nextLong(): Long = {
    var localU = u
    var localV = v
    var localW = w

    localU = localU * 2862933555777941757L + 7046029254386353087L
    localV ^= localV >>> 17
    localV ^= localV << 31
    localV ^= localV >>> 8
    localW = 4294957665L * (localW & 0xffffffffl) + (localW >>> 32)
    var x = localU ^ (localU << 21)
    x ^= x >>> 35
    x ^= x << 4
    val ret = (x + localV) ^ localW
    u = localU
    v = localV
    w = localW
    ret
  }

  def nextInt(bound: Int): Int = positiveModulo(nextLong().asInstanceOf[Int], bound)

  private def positiveModulo(i: Int, n: Int): Int = (i % n + n) % n
}
