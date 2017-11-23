namespace Sitecore.Support.Publishing.Service.Client.Services
{
  using Sitecore.Pipelines;
  using System.Web.Routing;
  using System.Web.Http;
  public class RegisterWebAPIRoutes
  {
    public virtual void Process(PipelineArgs args)
    {
      RouteTable.Routes.MapHttpRoute(
        name: "SupportPublishingJob",
        routeTemplate: "sitecore/api/ssc/publishing/jobs/{id}/{action}",
        defaults: new { controller = "SupportPublishingJob", action = "DefaultAction" });
    }
  }
}