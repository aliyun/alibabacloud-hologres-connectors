package com.alibaba.hologres.performace.params;

import java.util.concurrent.ThreadLocalRandom;

public class IntRandomParamProvider implements ParamProvider<Integer> {
  int min;
  int max;

  @Override
  public void init(String pattern) {
    String[] temp = pattern.split("-");
    min = Integer.parseInt(temp[0]);
    max = Integer.parseInt(temp[1]);
  }

  @Override
  public Integer next() {
    return ThreadLocalRandom.current().nextInt(min, max);
  }
}
