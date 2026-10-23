window.ui = SwaggerUIBundle({
  url: "../openapi.json",
  dom_id: "#swagger-ui",
  deepLinking: true,
  persistAuthorization: false,
  queryConfigEnabled: false,
  validatorUrl: null,
  supportedSubmitMethods: ["get", "post"],
  presets: [SwaggerUIBundle.presets.apis],
  layout: "BaseLayout"
});
