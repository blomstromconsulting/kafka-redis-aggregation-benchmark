package io.aggbench.ml;

import io.aggbench.domain.Hashing;
import io.aggbench.domain.InferenceModel;
import io.aggbench.domain.Json;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
class InferenceResourceTest {

    @Test
    void returnsDeterministicScoreAfterSimulatedLatency() {
        double[] features = InferenceModel.features(Hashing.sha256("aggregate"));
        String body = Json.toString(Map.of("id", "x", "inputs", List.of(Map.of("name", "features",
                "shape", List.of(1, 8), "datatype", "FP64", "data", Arrays.stream(features).boxed().toList()))));
        long start = System.nanoTime();
        double score = given().contentType(ContentType.JSON).body(body)
                .post("/v2/models/" + InferenceModel.MODEL_NAME + "/infer")
                .then().statusCode(200)
                .body("model_name", equalTo(InferenceModel.MODEL_NAME))
                .extract().jsonPath().getDouble("outputs[0].data[0]");
        assertEquals(InferenceModel.score(features), score, 1e-6);
        assertTrue((System.nanoTime() - start) / 1_000_000 >= 45, "fixed 50 ms profile");
    }

    @Test
    void profileCanBeChangedAtRuntime() {
        String profile = Json.toString(new InferenceProfile("lognormal-tail", 20, 5, 0.01, 800, 0, 0, 5000, 2.0, 64, 100));
        given().contentType(ContentType.JSON).body(profile).post("/admin/profile").then().statusCode(200)
                .body("slowdownFactor", equalTo(2.0f));
        String reset = Json.toString(new InferenceProfile("fixed", 50, 10, 0, 0, 0, 0, 5000, 1.0, 256, 10000));
        given().contentType(ContentType.JSON).body(reset).post("/admin/profile").then().statusCode(200);
    }
}
