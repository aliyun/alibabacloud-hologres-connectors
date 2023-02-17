package com.alibaba.hologres.performace.params;

import java.util.concurrent.ThreadLocalRandom;

class LongRandomParamProvider implements ParamProvider<Long> {
  long min;
  long max;

  @Override
  public void init(String pattern) {
    String[] temp = pattern.split("-");
    min = Long.parseLong(temp[0]);
    max = Long.parseLong(temp[1]);
  }

  @Override
  public Long next() {
    return ThreadLocalRandom.current().nextLong(min, max);
  }
}