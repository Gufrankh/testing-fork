package com.clairvoyant.weather.document;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author Gufran Khan
 * @version 1.0
 * @date 17-08-2021 16:20
 */
@Document
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WeatherDetails {
    @Id
    private Long id;
    private String name;
    private Double temp;
    private Double feels_like;
}
