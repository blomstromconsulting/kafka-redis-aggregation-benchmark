package io.aggbench.persistence;

import io.aggbench.domain.Json;
import io.aggbench.domain.PersistenceApi.AggregateRecord;
import io.aggbench.domain.PersistenceApi.Receipt;
import io.quarkus.test.junit.QuarkusTest;
import io.restassured.http.ContentType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/** Idempotency contract of the persistence API, against a real PostgreSQL (Quarkus Dev Services). */
@QuarkusTest
class PersistenceResourceTest {

    private static Receipt receipt(String actionId, String hash) {
        return new Receipt("run-t", actionId, "INDIVIDUAL", "corr-1", 1, List.of("corr-1:1:v1"), hash, 3,
                1_000, 1_000, 2_000_000, 2_000_100, "a1", "pod");
    }

    private static AggregateRecord aggregate(String corr, String checksum, int version) {
        return new AggregateRecord("run-agg", corr, version, List.of("a", "b", "c"), checksum, "pa", "pb", "pc",
                0.5, "m", 10, 1, 1_000, 900, 2_000_000, 2_000_100, "b", "pod");
    }

    @Test
    void receiptsAreCreatedOnceThenDuplicateThenConflict() {
        String body = Json.toString(List.of(receipt("I:x1", "h1"), receipt("I:x2", "h2"), receipt("I:x1", "h1")));
        given().contentType(ContentType.JSON).body(body).post("/api/receipts").then().statusCode(200)
                .body("[0].status", equalTo("CREATED"))
                .body("[1].status", equalTo("CREATED"))
                .body("[2].status", equalTo("DUPLICATE"));
        given().contentType(ContentType.JSON).body(Json.toString(List.of(receipt("I:x1", "h1")))).post("/api/receipts")
                .then().statusCode(200).body("[0].status", equalTo("DUPLICATE"));
        given().contentType(ContentType.JSON).body(Json.toString(List.of(receipt("I:x1", "DIFFERENT")))).post("/api/receipts")
                .then().statusCode(200).body("[0].status", equalTo("CONFLICT"));
    }

    @Test
    void finalAggregatesAreIdempotentByCorrelationIdAndVersion() {
        given().contentType(ContentType.JSON).body(Json.toString(List.of(aggregate("c-1", "sum", 1)))).post("/api/aggregates")
                .then().statusCode(200).body("[0].status", equalTo("CREATED"));
        given().contentType(ContentType.JSON).body(Json.toString(List.of(aggregate("c-1", "sum", 1)))).post("/api/aggregates")
                .then().statusCode(200).body("[0].status", equalTo("DUPLICATE"));
        given().contentType(ContentType.JSON).body(Json.toString(List.of(aggregate("c-1", "sum", 2)))).post("/api/aggregates")
                .then().statusCode(200).body("[0].status", equalTo("CONFLICT"));
        given().get("/api/runs/run-agg/summary").then().statusCode(200)
                .body("aggregates", equalTo(1))
                .body("conflicts", equalTo(1));
    }
}
