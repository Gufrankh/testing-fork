package com.clairvoyant.weather.controller;

import com.clairvoyant.weather.document.WeatherDetails;
import com.clairvoyant.weather.repository.WeatherRepository;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Instant;

import static com.clairvoyant.weather.contants.WeatherConstants.OPEN_WEATHER_COMMON_URL;
import static com.clairvoyant.weather.contants.WeatherConstants.WEATHER_END_POINT;

/**
 * @author Gufran Khan
 * @version 1.0
 * @date 17-08-2021 16:19
 */

@RestController
@Slf4j
public class WeatherController {

    @Autowired
    WeatherRepository weatherRepository;

    WebClient webClient = WebClient.create(OPEN_WEATHER_COMMON_URL);
    Instant start = Instant.now();


    /**
     * Created By Gufran Khan | 17-May-2021  |Get All Weather Details
     */
    @GetMapping(WEATHER_END_POINT)
    public Flux<WeatherDetails> getAllWeatherDetails() {
        log.info("Inside Class WeatherController Method getAllWeatherDetails");
        this.refreshData();
        return weatherRepository.findAll();
    }


    /**
     * Created By Gufran Khan | 17-May-2021  |Create Weather Detail
     */
    @PostMapping(WEATHER_END_POINT)
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<WeatherDetails> createWeatherDetails(@RequestBody Mono<WeatherDetails> weatherDetails) {
        log.info("Inside Class WeatherController Method createWeatherDetails");
        return weatherDetails.flatMap(result ->
                weatherRepository.save(result)
        );
    }

    /**
     * Created By Gufran Khan | 17-May-2021  |Update Book Detail By Id
     */
    @PutMapping(WEATHER_END_POINT)
    public Mono<ResponseEntity<WeatherDetails>> updateWeather(@RequestBody WeatherDetails weatherDetail) {

        log.info("Inside Class WeatherController Method updateWeather");
        return weatherRepository.findById(weatherDetail.getId())
                .flatMap(result -> {
                    result.setName(weatherDetail.getName());
                    return weatherRepository.save(result);
                })
                .map(updatedItem -> new ResponseEntity<>(updatedItem, HttpStatus.OK))
                .defaultIfEmpty(new ResponseEntity<>(HttpStatus.NOT_FOUND));
    }

    /**
     * Created By Gufran Khan | 17-May-2021  |Get Book Detail By City Name
     */
    @GetMapping(WEATHER_END_POINT + "/city/{city}")
    public Mono<WeatherDetails> getWeatherDetailsByCity(@PathVariable String city) {
        log.info("Inside Class WeatherController Method getWeatherDetailsByCity");
        return weatherRepository.findByName(city);
    }

    /**
     * Created By Gufran Khan | 17-May-2021  |Delete Weather Detail By Id
     */
    @DeleteMapping(WEATHER_END_POINT + "/{id}")
    public Mono<Void> deleteWeatherDetails(@PathVariable Long id) {
        log.info("Inside Class WeatherController Method deleteWeatherDetails");
        return weatherRepository.deleteById(id);
    }

    public void refreshData() {


        webClient.get().
                uri("/find?lat=19.076090&lon=72.877426&cnt=10&appid=6aa468dc488d1c9a0acc7bf6c9bee316").
                retrieve().bodyToMono(String.class).subscribe(v -> {
            JSONObject jsonObject = new JSONObject(v);
            if (jsonObject.getString("cod").equals("200")) {
                JSONArray arr = jsonObject.getJSONArray("list");
                arr.forEach(item -> {
                    WeatherDetails weatherDetails = new WeatherDetails();
                    JSONObject obj = (JSONObject) item;
                    JSONObject main = new JSONObject();
                    weatherDetails.setId(obj.getLong("id"));
                    weatherDetails.setName(obj.getString("name"));
                    if (obj.getJSONObject("main") != null) {
                        JSONObject mainObject = obj.getJSONObject("main");
                        weatherDetails.setTemp(mainObject.getDouble("temp"));
                        weatherDetails.setFeels_like(mainObject.getDouble("feels_like"));
                    }

                    weatherRepository.save(weatherDetails).subscribeOn(Schedulers.parallel()).subscribe();


                });

            }


        });


        //start = Instant.now();

    }


}

