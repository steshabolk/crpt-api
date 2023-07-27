package com.task;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

public class CrptApi {

    private final TimeUnit timeUnit;
    private final int requestLimit;
    private final Semaphore semaphore;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CloseableHttpClient httpClient = HttpClients.createDefault();
    private final Token token = new Token();

    public CrptApi(TimeUnit timeUnit, int requestLimit) {
        if (requestLimit <= 0) {
            throw new RuntimeException("Количество запросов должно быть положительным числом");
        }
        this.timeUnit = timeUnit;
        this.requestLimit = requestLimit;
        this.semaphore = new Semaphore(requestLimit, true);
        initSchedule();
    }

    public CreateDocumentResponse createDocumentForIntroduceGoods(IntroduceGoodsDocument document, String signature) throws InterruptedException {
        semaphore.acquire();
        try {
            String requestBody = objectMapper.writeValueAsString(new CreateDocumentRequestBody(
                    DocumentFormat.MANUAL, toBase64(document), null, signature, Type.LP_INTRODUCE_GOODS));

//            запрос с необходимым параметром pg для указания товарной группы товара,
//            его значение можно, например, дополнительно передавать в параметры метода
//            HttpPost postRequest = new HttpPost(new URIBuilder(RequestURL.DOCUMENT_CREATION)
//                    .addParameter("pg", ProductGroup.name()).build());

            HttpPost postRequest = new HttpPost(RequestURL.DOCUMENT_CREATION);
            postRequest.setHeader("content-type", "application/json");
            postRequest.setHeader("Authorization", token.getAuthHeader(signature));
            postRequest.setEntity(new StringEntity(requestBody));
            CloseableHttpResponse response = httpClient.execute(postRequest);
            if (isErrorStatusCode(response.getStatusLine().getStatusCode())) {
                handleErrorResponse(response);
            }
            return objectMapper.readValue(EntityUtils.toString(response.getEntity()), CreateDocumentResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handleErrorResponse(CloseableHttpResponse response) {
        try {
            ErrorResponse errorResponse = objectMapper.readValue(EntityUtils.toString(response.getEntity()), ErrorResponse.class);
            throw new RuntimeException(String.format("Ошибка при создании документа: код: %s, сообщение: %s, описание: %s",
                    errorResponse.code, errorResponse.errorMessage, errorResponse.description));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private boolean isErrorStatusCode(int statusCode) {
        int[] successful = new int[] {200, 201, 202};
        return Arrays.stream(successful).noneMatch(i -> i == statusCode);
    }

    private String toBase64(Object obj) {
        try {
            return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(obj));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private void initSchedule() {
        scheduler.scheduleAtFixedRate(() -> semaphore.release(requestLimit - semaphore.availablePermits()),
                1L, 1L, timeUnit);
    }

    static class RequestURL {

        private static final String BASE_URL = "https://ismp.crpt.ru/api/v3";
        private static final String DOCUMENT_CREATION = BASE_URL + "/lk/documents/create";
        private static final String AUTH_KEY = BASE_URL + "/auth/cert/key";
        private static final String AUTH_TOKEN = BASE_URL + "/auth/cert/";

    }

    class Token {

        private String token = null;
        private long expirationTime;
        private static final long TOKEN_LIFETIME_HOURS = 10;
        private static final String HEADER_PREFIX = "Bearer ";

        private String getAuthHeader(String signature) {
            if (token == null || !isValid()) {
                AuthData unsigned = getAuthKey();
                AuthData signed = signAuthData(unsigned, signature);
                getAuthToken(signed);
            }
            return HEADER_PREFIX + token;
        }

        private AuthData getAuthKey() {
            HttpGet getRequest = new HttpGet(RequestURL.AUTH_KEY);
            try {
                return httpClient.execute(getRequest, httpResponse ->
                        objectMapper.readValue(EntityUtils.toString(httpResponse.getEntity()), AuthData.class));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private AuthData signAuthData(AuthData unsigned, String signature) {
            // подписать поле data УКЭП
            unsigned.setData(toBase64(unsigned.getData()));
            return unsigned;
        }

        private void getAuthToken(AuthData signed) {
            try {
                String requestBody = objectMapper.writeValueAsString(signed);
                HttpPost postRequest = new HttpPost(RequestURL.AUTH_TOKEN);
                postRequest.setHeader("content-type", "application/json;charset=UTF-8");
                postRequest.setEntity(new StringEntity(requestBody));
                CloseableHttpResponse response = httpClient.execute(postRequest);
                if (isErrorStatusCode(response.getStatusLine().getStatusCode())) {
                    handleErrorResponse(response);
                }
                token = objectMapper.readTree(EntityUtils.toString(response.getEntity())).get("token").asText();
                expirationTime = Instant.now().plus(TOKEN_LIFETIME_HOURS, ChronoUnit.HOURS).toEpochMilli();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private boolean isValid() {
            return Instant.now().toEpochMilli() < expirationTime;
        }

    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class AuthData {

        private String uuid;

        private String data;

        public String getUuid() {
            return uuid;
        }

        public void setUuid(String uuid) {
            this.uuid = uuid;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
            this.data = data;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class CreateDocumentResponse {

        private String value;

        public void setValue(String value) {
            this.value = value;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class ErrorResponse {

        private String code;

        @JsonProperty("error_message")
        private String errorMessage;

        private String description;

        public void setCode(String code) {
            this.code = code;
        }

        public void setErrorMessage(String errorMessage) {
            this.errorMessage = errorMessage;
        }

        public void setDescription(String description) {
            this.description = description;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class CreateDocumentRequestBody {

        @JsonProperty("document_format")
        private DocumentFormat documentFormat;

        @JsonProperty("product_document")
        private String productDocument;

        @JsonProperty("product_group")
        private ProductGroup productGroup;

        private String signature;

        private Type type;

        public CreateDocumentRequestBody(DocumentFormat documentFormat, String productDocument,
                                         ProductGroup productGroup, String signature, Type type) {
            this.documentFormat = documentFormat;
            this.productDocument = productDocument;
            this.productGroup = productGroup;
            this.signature = signature;
            this.type = type;
        }

        public DocumentFormat getDocumentFormat() {
            return documentFormat;
        }

        public String getProductDocument() {
            return productDocument;
        }

        public ProductGroup getProductGroup() {
            return productGroup;
        }

        public String getSignature() {
            return signature;
        }

        public Type getType() {
            return type;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class IntroduceGoodsDocument {

        private Description description;

        @JsonProperty("doc_id")
        private String docId;

        @JsonProperty("doc_status")
        private String docStatus;

        @JsonProperty("doc_type")
        private String docType;

        private boolean importRequest;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("participant_inn")
        private String participantInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private LocalDate productionDate;

        @JsonProperty("production_type")
        private ProductionType productionType;

        private List<Product> products;

        @JsonProperty("reg_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private LocalDate regDate;

        @JsonProperty("reg_number")
        private String regNumber;

        public IntroduceGoodsDocument(Description description, String docId, String docStatus, String docType,
                                      boolean importRequest, String ownerInn, String participantInn, String producerInn,
                                      LocalDate productionDate, ProductionType productionType, List<Product> products,
                                      LocalDate regDate, String regNumber) {
            this.description = description;
            this.docId = docId;
            this.docStatus = docStatus;
            this.docType = docType;
            this.importRequest = importRequest;
            this.ownerInn = ownerInn;
            this.participantInn = participantInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.productionType = productionType;
            this.products = products;
            this.regDate = regDate;
            this.regNumber = regNumber;
        }

        public Description getDescription() {
            return description;
        }

        public String getDocId() {
            return docId;
        }

        public String getDocStatus() {
            return docStatus;
        }

        public String getDocType() {
            return docType;
        }

        public boolean isImportRequest() {
            return importRequest;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public ProductionType getProductionType() {
            return productionType;
        }

        public List<Product> getProducts() {
            return products;
        }

        public LocalDate getRegDate() {
            return regDate;
        }

        public String getRegNumber() {
            return regNumber;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class Description {

        private String participantInn;

        public Description(String participantInn) {
            this.participantInn = participantInn;
        }

        public String getParticipantInn() {
            return participantInn;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    class Product {

        @JsonProperty("certificate_document")
        private CertificateDocument certificateDocument;

        @JsonProperty("certificate_document_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private LocalDate certificateDocumentDate;

        @JsonProperty("certificate_document_number")
        private String certificateDocumentNumber;

        @JsonProperty("owner_inn")
        private String ownerInn;

        @JsonProperty("producer_inn")
        private String producerInn;

        @JsonProperty("production_date")
        @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd")
        private LocalDate productionDate;

        @JsonProperty("tnved_code")
        private String tnvedCode;

        @JsonProperty("uit_code")
        private String uitCode;

        @JsonProperty("uitu_code")
        private String uituCode;

        public Product(CertificateDocument certificateDocument, LocalDate certificateDocumentDate,
                       String certificateDocumentNumber, String ownerInn, String producerInn, LocalDate productionDate,
                       String tnvedCode, String uitCode, String uituCode) {
            this.certificateDocument = certificateDocument;
            this.certificateDocumentDate = certificateDocumentDate;
            this.certificateDocumentNumber = certificateDocumentNumber;
            this.ownerInn = ownerInn;
            this.producerInn = producerInn;
            this.productionDate = productionDate;
            this.tnvedCode = tnvedCode;
            this.uitCode = uitCode;
            this.uituCode = uituCode;
        }

        public CertificateDocument getCertificateDocument() {
            return certificateDocument;
        }

        public LocalDate getCertificateDocumentDate() {
            return certificateDocumentDate;
        }

        public String getCertificateDocumentNumber() {
            return certificateDocumentNumber;
        }

        public String getOwnerInn() {
            return ownerInn;
        }

        public String getProducerInn() {
            return producerInn;
        }

        public LocalDate getProductionDate() {
            return productionDate;
        }

        public String getTnvedCode() {
            return tnvedCode;
        }

        public String getUitCode() {
            return uitCode;
        }

        public String getUituCode() {
            return uituCode;
        }
    }

    enum DocumentFormat {
        MANUAL, XML, CSV
    }

    enum ProductGroup {
        clothes, shoes, tobacco, perfumery, tires, electronics, pharma, milk, bicycle, wheelchairs
    }

    enum Type {
        LP_INTRODUCE_GOODS, LP_INTRODUCE_GOODS_CSV, LP_INTRODUCE_GOODS_XML
    }

    enum ProductionType {
        OWN_PRODUCTION, CONTRACT_PRODUCTION
    }

    enum CertificateDocument {
        CONFORMITY_CERTIFICATE, CONFORMITY_DECLARATION,
    }

}
