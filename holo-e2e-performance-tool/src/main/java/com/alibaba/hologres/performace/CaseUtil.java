package com.alibaba.hologres.performace;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class CaseUtil {
  public static void main(String[] args) throws IOException {
    File file = new File(args[0]);

    List<String[]> conditions = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {
      String line;
      while ((line = br.readLine()) != null) {
        line = line.trim();
        if (line.length() == 0) {
          continue;
        }
        String[] cond = line.split(" ");
        if (cond.length < 2) {
          continue;
        }
        conditions.add(cond);
      }
    }
    String[] values = new String[conditions.size()];
    for (int i = 0; i < values.length; ++i) {
      values[i] = null;
    }

    walk(conditions, 0, values, (value) -> {
      StringBuilder fileName = new StringBuilder();
      for (int i = 0; i < value.length; ++i) {
        if (i > 0) {
          fileName.append("_");
        }
        fileName.append(value[i]);
      }
      File caseFile = new File(fileName.toString());
      if (caseFile.exists()) {
        deleteFile(caseFile);
      }
      caseFile.mkdirs();
      File tail = new File(caseFile, "tail.conf");
      try (BufferedWriter bw = new BufferedWriter(
          new OutputStreamWriter(new FileOutputStream(tail)))) {
        for (int i = 0; i < value.length; ++i) {
          bw.write(conditions.get(i)[0] + "=" + value[i]);
          bw.newLine();
        }
      } catch (Exception e) {
        e.printStackTrace();
      }

    });
  }

  private static void deleteFile(File file) {
    if (file.isDirectory()) {
      File[] files = file.listFiles();
      for (File f : files) {
        deleteFile(f);
      }
    } else {
      file.delete();
    }
  }

  private static void walk(List<String[]> conditions, int currentLevel, String[] values,
      Consumer<String[]> consumer) {
    String[] cond = conditions.get(currentLevel);
    for (int i = 1; i < cond.length; ++i) {
      values[currentLevel] = cond[i];
      if (currentLevel == conditions.size() - 1) {
        consumer.accept(values);
      } else {
        walk(conditions, currentLevel + 1, values, consumer);
      }
    }

  }
}