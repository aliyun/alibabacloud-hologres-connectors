package com.alibaba.hologres.performace.params;

import java.util.ArrayList;
import java.util.List;

public class ParamsProvider {
  List<ParamProvider> list;

  public ParamsProvider(String pattern) {
    String[] temp = pattern.split(",");
    list = new ArrayList<>();
    for (String str : temp) {
      switch (str.charAt(0)) {
        case 'I':
          IntRandomParamProvider p = new IntRandomParamProvider();
          p.init(str.substring(1));
          list.add(p);
          break;
        case 'L':
          LongRandomParamProvider lp = new LongRandomParamProvider();
          lp.init(str.substring(1));
          list.add(lp);
          break;
        default:
          throw new RuntimeException("unknown pattern " + str.charAt(0));
      }
    }
  }

  public Object get(int index) {
    return list.get(index).next();
  }

  public int size() {
    return list.size();
  }
}
