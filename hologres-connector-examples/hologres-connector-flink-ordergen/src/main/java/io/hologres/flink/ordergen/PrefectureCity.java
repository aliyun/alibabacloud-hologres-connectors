package io.hologres.flink.ordergen;

import java.io.Serializable;
import java.util.List;

/**
 * PrefectureCity.
 */
public class PrefectureCity implements Serializable {
    private String prefectureNameZh;
    private String prefectureName;
    private List<City> cities;

    public PrefectureCity(String prefectureNameZh, String prefectureName, List<City> cities) {
        this.prefectureNameZh = prefectureNameZh;
        this.prefectureName = prefectureName;
        this.cities = cities;
    }

    public List<City> getCities() {
        return cities;
    }

    public void setCities(List<City> cities) {
        this.cities = cities;
    }

    public String getPrefectureNameZh() {
        return prefectureNameZh;
    }
}
