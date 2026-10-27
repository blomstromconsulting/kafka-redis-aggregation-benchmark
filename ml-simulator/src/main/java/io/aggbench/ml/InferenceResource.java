package io.aggbench.ml;

import io.aggbench.domain.InferenceModel;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.List;
import java.util.Map;

/**
 * KServe Open Inference Protocol (v2) subset: health, model metadata and {@code infer}. A real KServe
 * InferenceService exposing the same model name can replace this simulator by changing {@code INFERENCE_URL}.
 * The score is a deterministic function of the input features ({@link InferenceModel}).
 */
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class InferenceResource {

    /** v2 inference request. */
    public record InferRequest(String id, List<Tensor> inputs) {
    }

    /** v2 tensor. */
    public record Tensor(String name, List<Integer> shape, String datatype, List<Double> data) {
    }

    @Inject
    ModelServer server;

    @GET
    @Path("v2/health/live")
    public Map<String, Object> live() {
        return Map.of("live", true);
    }

    @GET
    @Path("v2/health/ready")
    public Map<String, Object> ready() {
        return Map.of("ready", true);
    }

    @GET
    @Path("v2/models/{model}")
    public Response metadata(@PathParam("model") String model) {
        if (!InferenceModel.MODEL_NAME.equals(model)) {
            return Response.status(404).entity(Map.of("error", "unknown model " + model)).build();
        }
        return Response.ok(Map.of("name", model, "platform", "simulator", "versions", List.of("1"),
                "inputs", List.of(Map.of("name", "features", "datatype", "FP64", "shape", List.of(-1, InferenceModel.FEATURES))),
                "outputs", List.of(Map.of("name", "score", "datatype", "FP64", "shape", List.of(-1, 1))))).build();
    }

    @GET
    @Path("v2/models/{model}/ready")
    public Map<String, Object> modelReady(@PathParam("model") String model) {
        return Map.of("name", model, "ready", InferenceModel.MODEL_NAME.equals(model));
    }

    @POST
    @Path("v2/models/{model}/infer")
    public Uni<Response> infer(@PathParam("model") String model, InferRequest request) {
        if (!InferenceModel.MODEL_NAME.equals(model) || request.inputs() == null || request.inputs().isEmpty()) {
            return Uni.createFrom().item(Response.status(400).entity(Map.of("error", "bad request")).build());
        }
        double[] features = request.inputs().getFirst().data().stream().mapToDouble(Double::doubleValue).toArray();
        return server.infer(() -> InferenceModel.score(features))
                .map(score -> Response.ok(Map.of(
                        "model_name", model,
                        "model_version", "1",
                        "id", request.id() == null ? "" : request.id(),
                        "outputs", List.of(Map.of("name", "score", "shape", List.of(1, 1), "datatype", "FP64",
                                "data", List.of(score))))).build())
                .onFailure(ModelServer.Overloaded.class).recoverWithItem(e -> Response.status(503).entity(Map.of("error", e.getMessage())).build())
                .onFailure().recoverWithItem(e -> Response.status(500).entity(Map.of("error", e.getMessage())).build());
    }

    @GET
    @Path("admin/profile")
    public InferenceProfile profile() {
        return server.profile();
    }

    @POST
    @Path("admin/profile")
    public InferenceProfile updateProfile(InferenceProfile next) {
        server.update(next);
        return server.profile();
    }
}
