namespace Sitecore.Support.Publishing.Service.Client.Services.Controllers
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Net;
  using System.Net.Http;
  using System.Net.Http.Formatting;
  using System.Threading.Tasks;
  using System.Web.Http;

  using Sitecore.Abstractions;
  using Sitecore.Data.Managers;
  using Sitecore.Framework.Conditions;
  using Sitecore.Framework.Publishing.PublishJobQueue;
  using Sitecore.Publishing.Service.Client.Services.Attributes;
  using Sitecore.Publishing.Service.Client.Services.Contracts;
  using Sitecore.Publishing.Service.Client.Services.Data;
  using Sitecore.Publishing.Service.Client.Services.Model;
  using Sitecore.Publishing.Service.JobQueue;
  using Sitecore.Publishing.Service.Manifest;
  using Sitecore.Publishing.Service.Security;
  using Sitecore.Publishing.Service.SitecoreAbstractions;
  using Sitecore.Services.Core;
  using Sitecore.Services.Core.ComponentModel.DataAnnotations;
  using Sitecore.Services.Infrastructure.Services;
  using Sitecore.Services.Infrastructure.Sitecore.Services;
  using Sitecore.Publishing.Service;
  using Sitecore.Publishing.Service.Client.Services;

  [Authorize]
  [ServicesController("Publishing.Jobs")]
  public class SupportPublishingJobController : EntityService<PublishingJobEntity>
  {
    private const int DefaultPageSize = 10;

    private readonly IPublishRepository<PublishingJobEntity> _repository;
    private readonly IPublishJobQueueServiceFactory _publishJobQueueServiceFactory;
    private readonly IClientUiMessages _clientUiMessages;
    private readonly IUserRoleService _userRoleService;
    private readonly IUserProfile _userProfile;

    public SupportPublishingJobController() : this(
            new Sitecore.Support.Publishing.Service.Client.Services.Data.PublishingJobRepository(
                new SitecoreSettingsWrapper(),
                new PublishingJobProviderFactory(),
                new ManifestProviderFactory(), 
                new DatabaseFactoryWrapper(new PublishingLogWrapper()),
                new ClientUiMessages(new FactoryWrapper()),
                new UserProfileWrapper(new LanguageManagerWrapper())
                ),
            new PublishJobQueueServiceFactory(),
            new ClientUiMessages(new FactoryWrapper()),
            new UserRoleService(),
            new UserProfileWrapper(new LanguageManagerWrapper()))
        {
    }

    public SupportPublishingJobController
        (
            IPublishRepository<PublishingJobEntity> repository,
            IPublishJobQueueServiceFactory publishJobQueueServiceFactory,
            IClientUiMessages clientUiMessages,
            IUserRoleService userRoleService,
            IUserProfile userProfileService
        ) : base(repository)
        {
      Condition.Requires(repository, "repository").IsNotNull();
      Condition.Requires(publishJobQueueServiceFactory, "publishJobQueueServiceFactory").IsNotNull();
      Condition.Requires(clientUiMessages, "clientUiMessages").IsNotNull();
      Condition.Requires(userRoleService, "userRoleService").IsNotNull();
      Condition.Requires(userProfileService, "userProfileService").IsNotNull();

      _repository = repository;
      _publishJobQueueServiceFactory = publishJobQueueServiceFactory;
      _clientUiMessages = clientUiMessages;
      _userRoleService = userRoleService;
      _userProfile = userProfileService;
    }

    public SupportPublishingJobController
        (
            IPublishRepository<PublishingJobEntity> repository,
            IMetaDataBuilder metaDataBuilder,
            IEntityValidator entityValidator,
            IPublishJobQueueServiceFactory publishJobQueueServiceFactory,
            IClientUiMessages clientUiMessages,
            IUserRoleService userRoleService,
            IUserProfile userProfileService
        ) : base(repository, metaDataBuilder, entityValidator)
        {
      Condition.Requires(repository, "repository").IsNotNull();
      Condition.Requires(publishJobQueueServiceFactory, "publishJobQueueServiceFactory").IsNotNull();
      Condition.Requires(clientUiMessages, "clientUiMessages").IsNotNull();
      Condition.Requires(userRoleService, "userRoleService").IsNotNull();
      Condition.Requires(userProfileService, "userProfileService").IsNotNull();

      _repository = repository;
      _publishJobQueueServiceFactory = publishJobQueueServiceFactory;
      _clientUiMessages = clientUiMessages;
      _userRoleService = userRoleService;
      _userProfile = userProfileService;
    }

    [HttpPost]
    [DenyUnauthorisedRoles]
    public HttpResponseMessage FullPublish([FromBody] PublishingJobEntity entity)
    {
      var response = base.CreateEntity(entity);

      response.Content = new ObjectContent<PublishingJobEntity>(entity, new JsonMediaTypeFormatter(), "application/json");

      return response;
    }

    [HttpPost]
    public HttpResponseMessage ItemPublish([FromBody] PublishingJobEntity entity)
    {
      Condition.Requires(entity.ItemId, "itemId").IsNotNull();

      var response = base.CreateEntity(entity);

      response.Content = new ObjectContent<PublishingJobEntity>(entity, new JsonMediaTypeFormatter(), "application/json");

      return response;
    }

    [HttpPost]
    public HttpResponseMessage RePublish([FromBody] Guid jobId)
    {
      var previousJob = _repository.FindById(jobId.ToString());

      if (previousJob == null)
      {
        return new EntityNotFoundErrorResponse();
      }

      var canRePublish = UserCanRePublish(previousJob);

      if (canRePublish)
      {
        var newJob = new PublishingJobEntity()
        {
          Id = null,
          Metadata = previousJob.Metadata,
          ItemId = previousJob.ItemId,
          IncludeDescendantItems = previousJob.IncludeDescendantItems,
          IncludeRelatedItems = previousJob.IncludeRelatedItems,
          Languages = previousJob.Languages,
          RequestedBy = _userRoleService.GetCurrentUsername(),
          SourceDatabase = previousJob.SourceDatabase,
          SynchroniseWithTarget = previousJob.SynchroniseWithTarget,
          Targets = previousJob.Targets
        };

        var response = CreateEntity(newJob);

        response.Content = new ObjectContent<PublishingJobEntity>(newJob, new JsonMediaTypeFormatter(), "application/json");

        return response;
      }
      else
      {
        return new HttpResponseMessage() { StatusCode = HttpStatusCode.BadRequest, ReasonPhrase = "User not allowed to republish this job" };
      }
    }

    [HttpGet]
    public bool CanRePublish(Guid id)
    {
      var previousJob = _repository.FindById(id.ToString());

      if (previousJob == null)
      {
        return false;
      }

      return UserCanRePublish(previousJob);
    }

    [HttpGet]
    public async Task<PublishingJobData> All(string sourceDatabase)
    {
      var messages = new List<Message>();

      await CheckStatus(messages);

      if (ServiceStatusIsError(messages))
      {
        return new PublishingJobData()
        {
          Messages = messages,
          Active = Enumerable.Empty<PublishingJobEntity>(),
          Queued = Enumerable.Empty<PublishingJobEntity>(),
          Recent = Enumerable.Empty<PublishingJobEntity>()
        };
      }

      _repository.SetDatabaseContext(sourceDatabase);

      var activeJobTask = _repository.GetActive();
      var recentJobsTask = _repository.GetRecent(0, DefaultPageSize);
      var queuedJobsTask = _repository.GetQueue(0, DefaultPageSize);

      await Task.WhenAll(
          activeJobTask,
          recentJobsTask,
          queuedJobsTask);

      var activeJob = activeJobTask.Result;
      var queuedJobs = queuedJobsTask.Result;
      var recentJobs = recentJobsTask.Result;

      return new PublishingJobData()
      {
        Messages = messages,
        Active = activeJob != null ? new List<PublishingJobEntity>() { activeJob } : Enumerable.Empty<PublishingJobEntity>(),
        Queued = queuedJobs,
        Recent = recentJobs
      };
    }

    [HttpGet]
    public async Task<PublishingJobData> Active()
    {
      var messages = new List<Message>();

      await CheckStatus(messages);

      var activeJob = await _repository.GetActive();

      return new PublishingJobData()
      {
        Messages = messages,
        Active = activeJob != null ? new List<PublishingJobEntity>() { activeJob } : Enumerable.Empty<PublishingJobEntity>(),
        Queued = Enumerable.Empty<PublishingJobEntity>()
      };
    }

    [HttpGet]
    public async Task<PublishingJobData> Queued(int top = DefaultPageSize)
    {
      var publishJobData = new PublishingJobData();

      var messages = new List<Message>();

      await CheckStatus(messages);

      publishJobData.Messages = messages;
      publishJobData.Recent = Enumerable.Empty<PublishingJobEntity>();
      publishJobData.Active = Enumerable.Empty<PublishingJobEntity>();

      if (ServiceStatusIsError(messages))
      {
        publishJobData.Queued = Enumerable.Empty<PublishingJobEntity>();
      }
      else
      {
        publishJobData.Queued = await _repository.GetQueue(0, top);
      }

      return publishJobData;
    }

    [HttpGet]
    public async Task<PublishingJobData> Recent(int top = DefaultPageSize)
    {
      var messages = new List<Message>();

      await CheckStatus(messages);

      var recentJobs = await _repository.GetRecent(0, top);

      return new PublishingJobData()
      {
        Messages = messages,
        Active = Enumerable.Empty<PublishingJobEntity>(),
        Recent = recentJobs,
      };
    }

    [HttpGet]
    public async Task<ServiceStatus> Status()
    {
      var messages = new List<Message>();

      await CheckStatus(messages);

      return new ServiceStatus(!messages.Any())
      {
        Messages = messages
      };
    }

    private bool ServiceStatusIsError(List<Message> messages)
    {
      return messages.Any(m => m.Type == MessageType.Error.DisplayName);
    }

    private async Task CheckStatus(IList<Message> errorMessages)
    {
      var status = await _publishJobQueueServiceFactory.GetPublishJobQueueService().Status();
      var clientLanguage = _userProfile.GetClientLanguage() ?? Context.Language;

      var message = _clientUiMessages.GetMessageText(ConfigurationConstants.PublishServiceDownMessage, ConfigurationConstants.TextField, clientLanguage);

      if (status.Status == PublishJobQueueServiceState.Error)
      {
        errorMessages.Add(new Message()
        {
          Text = message,
          Type = MessageType.Error.DisplayName
        });
      }
    }

    private bool UserCanRePublish(PublishingJobEntity previousJob)
    {
      var currentUser = this._userRoleService.GetCurrentUsername();

      var isAdministrator = _userRoleService.IsCurrentUserAdministrator();
      var isSameUser = currentUser.Equals(previousJob.RequestedBy, StringComparison.InvariantCultureIgnoreCase);

      return isAdministrator || isSameUser;
    }
  }
}