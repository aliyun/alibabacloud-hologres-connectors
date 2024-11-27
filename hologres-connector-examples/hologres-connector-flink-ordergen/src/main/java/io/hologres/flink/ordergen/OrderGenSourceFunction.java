package io.hologres.flink.ordergen;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;

import com.github.javafaker.Faker;
import com.google.gson.Gson;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.Random;

/**
 * OrdersSourceFunction.
 */
public class OrderGenSourceFunction extends RichParallelSourceFunction<RowData> {
    private final int arity = 11;
    private transient Faker faker;
    private Province[] provinces;
    private Random random = new Random();
    private boolean shouldContinue = true;

    private void init() {
        faker = new Faker(new Locale("zh-CN"), random);
        Gson gson = new Gson();
        ClassLoader classLoader = Province.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream("china_cities.json");
        Reader reader = new InputStreamReader(inputStream);
        provinces = gson.fromJson(reader, Province[].class);
    }

    @Override
    public void run(SourceContext<RowData> sourceContext) throws Exception {
        if (faker == null) {
            init();
        }
        while (shouldContinue) {
            int cityIdx = Math.abs(random.nextInt());
            Province province = provinces[cityIdx % provinces.length];
            PrefectureCity prefectureCity =
                    province.getPrefectureCities()
                            .get(cityIdx % province.getPrefectureCities().size());
            City city = prefectureCity.getCities().get(cityIdx % prefectureCity.getCities().size());

            GenericRowData row = new GenericRowData(arity);
            row.setField(0, Math.abs(random.nextLong())); // user id
            row.setField(1, StringData.fromString(faker.name().name())); // user name
            row.setField(2, Math.abs(random.nextLong())); // item id
            row.setField(3, StringData.fromString(faker.commerce().productName())); // item name
            row.setField(
                    4,
                    DecimalData.fromBigDecimal(
                            new BigDecimal(faker.commerce().price()), 38, 12)); // price
            row.setField(5, StringData.fromString(province.getProvinceNameZh())); // province
            row.setField(6, StringData.fromString(prefectureCity.getPrefectureNameZh())); // city
            row.setField(7, StringData.fromString(city.getLongtitude())); // city longitude
            row.setField(8, StringData.fromString(city.getLatitude())); // city latitude
            row.setField(9, StringData.fromString(faker.internet().ipV4Address())); // ip
            row.setField(10, TimestampData.fromEpochMillis(System.currentTimeMillis())); // time
            sourceContext.collect(row);
        }
    }

    @Override
    public void cancel() {
        shouldContinue = false;
    }
}
