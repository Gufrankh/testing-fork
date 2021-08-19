package com.clairvoyant.weather.repository;

import com.clairvoyant.weather.document.WeatherDetails;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Gufran Khan
 * @version 1.0
 * @date 17-08-2021 16:21
 */
public interface WeatherRepository extends ReactiveMongoRepository<WeatherDetails,Long> {

    Mono<WeatherDetails> findByName(String city);
}
