package io.hologres.flink.ordergen;

import java.io.Serializable;
import java.util.List;

/** Province. */
public class Province implements Serializable {
    private String provinceNameZh;
    private String provinceName;
    private List<PrefectureCity> prefectureCities;

    public Province(
            String provinceNameZh, String provinceName, List<PrefectureCity> prefectureCities) {
        this.provinceNameZh = provinceNameZh;
        this.provinceName = provinceName;
        this.prefectureCities = prefectureCities;
    }

    public List<PrefectureCity> getPrefectureCities() {
        return prefectureCities;
    }

    public void setPrefectureCities(List<PrefectureCity> prefectureCities) {
        this.prefectureCities = prefectureCities;
    }

    public String getProvinceNameZh() {
        return provinceNameZh;
    }
}
