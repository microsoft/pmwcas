// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cstdint>
#include <random>

namespace pmwcas {

/// A fast random (32-bit) number generator, for a uniform distribution (all
/// numbers in the range are equally likely).
class RandomNumberGenerator {
 public:
  RandomNumberGenerator(uint32_t seed = 0, uint32_t min = 0, uint32_t max = 0) :
      max_{ max } {
    if(seed == 0) {
      std::random_device rd{};
      x_ = rd() & 0x0FFFFFFF;
    } else {
      x_ = seed;
    }
    y_ = 362436069;
    z_ = 521288629;
    w_ = 88675123;
  }

  uint32_t Generate() {
    uint32_t t;
    t = (x_ ^ (x_ << 11));
    x_ = y_;
    y_ = z_;
    z_ = w_;
    uint32_t result = (w_ = (w_ ^ (w_ >> 19)) ^ (t ^ (t >> 8)));
    return (max_ > 0) ? result % max_ : result;
  }

  uint32_t Generate(uint32_t max) {
    return Generate() % max;
  }

 private:
  uint32_t max_;
  uint32_t x_;
  uint32_t y_;
  uint32_t z_;
  uint32_t w_;
};

/// A random number generator, for a Zipfian distribution (smaller numbers are
/// most likely.) "The algorithm used here is from 'Quickly Generating
/// Billion-Record Synthetic Databases', Jim Gray et al, SIGMOD 1994."
class ZipfRandomNumberGenerator {
public:
  /// Create a zipfian generator for items between min and max.
  ZipfRandomNumberGenerator(uint32_t seed, uint32_t min, uint32_t max) :
    rng_{ seed },
    items_{ max - min + 1 },
    base_{ min },
    zipfianconstant_{ 0.99 },
    zetan_{ zetastatic(items_, zipfianconstant_) },
    half2theta_{}
  {
    init();
  }

  double zeta(uint32_t n, double theta) {
    countforzeta_ = n;
    return zetastatic(n, theta);
  }

  /// Compute the zeta constant needed for the distribution. Do this from
  /// scratch for a distribution with n items, using the zipfian constant theta.
  /// This is a static version of the function which will not remember n.
  static double zetastatic(uint32_t n, double theta) {
    return zetastatic(0, n, theta, 0);
  }

  /// Compute the zeta constant needed for the distribution. Do this
  /// incrementally for a distribution that has n items now but used to have
  /// st items. Use the zipfian constant theta. Remember the new value of n so
  /// that if we change the itemcount, we'll know to recompute zeta.
  double zeta(int64_t st, uint32_t n, double theta, double initialsum) {
    countforzeta_ = n;
    return zetastatic(st, n, theta, initialsum);
  }

  /// Compute the zeta constant needed for the distribution. Do this
  /// incrementally for a distribution that has n items now but used to have st
  /// items. Use the zipfian constant theta. Remember the new value of n so that
  /// if we change the itemcount, we'll know to recompute zeta.
  static double zetastatic(uint32_t start, uint32_t n, double theta,
      double initialsum) {
    double sum = initialsum;
    for (uint32_t i = start; i < n; ++i) {
      sum += 1 / (pow((double)(i + 1), theta));
    }
    return sum;
  }

  /// Generate the next value, skewed by the Zipfian distribution. The 0th item
  /// will be the most popular, followed by the 1st, followed by the 2nd, etc.
  /// (Or, if min != 0, the min-th item is the most popular, the min+1th item
  /// the next most popular, etc.) If you want the popular items scattered
  /// throughout the item space, use ScrambledZipfianGenerator instead.
  uint32_t Generate() {
    // From "Quickly Generating Billion-Record Synthetic Databases",
    // Jim Gray et al, SIGMOD 1994
    double u = static_cast<double>(rng_.Generate()) / (1llu << 32);
    double uz = u * zetan_;
    if (uz < 1.0) {
      return 0;
    } else if (uz < 1.0 + half2theta_) {
      return 1;
    } else {
      return base_ +
        static_cast<uint32_t>((items_)* pow(eta_*u - eta_ + 1, alpha_));
    }
  }

protected:
  void init() {
    theta_ = zipfianconstant_;
    half2theta_ = pow(0.5, theta_);
    zeta2theta_ = zeta(2, theta_);
    alpha_ = 1.0 / (1.0 - theta_);
    countforzeta_ = items_;
    eta_ = (1 - pow(2.0 / items_, 1 - theta_)) / (1 - zeta2theta_ / zetan_);

    Generate();
  }

protected:
  /// Number of items.
  uint32_t items_;

  /// Min item to generate.
  uint32_t base_;

  /// The zipfian constant to use.
  double zipfianconstant_;

  /// Computed parameters for generating the distribution.
  double alpha_;
  double zetan_;
  double eta_;
  double theta_;
  double half2theta_;
  double zeta2theta_;

  /// The number of items used to compute zetan the last time.
  uint32_t countforzeta_;

  RandomNumberGenerator rng_;
};

/// Same as ZipfianRandomNumberGenerator, except favors larger numbers rather
/// than smaller numbers.
class ReverseZipfRandomNumberGenerator : public ZipfRandomNumberGenerator {
public:
  /// Create a zipfian generator for items between min and max.
  ReverseZipfRandomNumberGenerator(uint32_t seed, uint32_t min, uint32_t max) :
    ZipfRandomNumberGenerator(seed, min, max)
  { }

  /// Generate the next value, skewed by the reverse-Zipfian distribution. The
  /// largest item will be the most popular, followed by the next largest, etc.
  uint32_t Generate() {
    uint32_t result = ZipfRandomNumberGenerator::Generate();
    return base_ + items_ - 1 - result;
  }
};

class ScrambledZipfRandomNumberGenerator : public ZipfRandomNumberGenerator {
public:
  /// Create a zipfian generator for items between min and max.
  ScrambledZipfRandomNumberGenerator(uint32_t seed, uint32_t min, uint32_t max) :
    ZipfRandomNumberGenerator(seed, min, max)
  { }

  /// Generate the next value, skewed by the reverse-Zipfian distribution. The
  /// largest item will be the most popular, followed by the next largest, etc.
  uint32_t Generate() {
    uint32_t result = ZipfRandomNumberGenerator::Generate();
    return base_ + FnvHash64(result) % items_;
  }

private:
  static uint64_t FnvHash64(uint64_t val)
  {
    //from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
    int64_t hashval = 0xCBF29CE484222325LL;

    for (int i = 0; i < 8; i++)
    {
      int32_t octet = val & 0x00ff;
      val = val >> 8;

      hashval = hashval ^ octet;
      hashval = hashval * 1099511628211LL;
    }

    return std::abs(hashval);
  }
};
} // namespace pmwcas
