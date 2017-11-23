namespace Sitecore.Support.Publishing.Service.Client.Services.Data
{
  using System;
  using System.Collections.Generic;
  using System.Linq;
  using System.Threading.Tasks;

  using Sitecore.Abstractions;
  using Sitecore.Data;
  using Sitecore.Data.Items;
  using Sitecore.Data.Managers;
  using Sitecore.Framework.Conditions;
  using Sitecore.Framework.Publishing.PublishJobQueue;
  using Sitecore.Framework.Publishing.Manifest;
  using Sitecore.Globalization;
  using Sitecore.Publishing.Service.Client.Services.Contracts;
  using Sitecore.Publishing.Service.Client.Services.Model;
  using Sitecore.Publishing.Service.JobQueue;
  using Sitecore.Publishing.Service.Manifest;
  using Sitecore.Publishing.Service.SitecoreAbstractions;

  using PublishManager = Sitecore.Publishing.PublishManager;
  using Sitecore.Publishing.Service.Client.Services;
  using Sitecore.Publishing.Service;

  public class PublishingJobRepository : IPublishRepository<PublishingJobEntity>
  {
    private readonly IPublishingJobProvider _jobProvider;
    private readonly IManifestProvider _manifestProvider;
    private readonly IDatabaseFactory _databaseFactory;
    private readonly ISitecoreSettings _settings;
    private readonly IClientUiMessages _clientUiMessages;

    private string _sourceDatabase;
    private readonly Language _language;

    public PublishingJobRepository() : this(
        new SitecoreSettingsWrapper(),
        new PublishingJobProviderFactory(),
        new ManifestProviderFactory(),
        new DatabaseFactoryWrapper(new PublishingLogWrapper()),
        new ClientUiMessages(new FactoryWrapper()),
        new UserProfileWrapper(new LanguageManagerWrapper())
        )
    {
    }

    public PublishingJobRepository(
        ISitecoreSettings settings,
        IPublishingJobProviderFactory jobProviderFactory,
        IManifestProviderFactory manifestProviderFactory,
        IDatabaseFactory databaseFactory,
        IClientUiMessages clientUiMessages,
        IUserProfile userProfile)
    {
      Condition.Requires(settings, "settings").IsNotNull();
      Condition.Requires(jobProviderFactory, "jobProviderFactory").IsNotNull();
      Condition.Requires(manifestProviderFactory, "manifestProviderFactory").IsNotNull();
      Condition.Requires(databaseFactory, "databaseFactory").IsNotNull();
      Condition.Requires(clientUiMessages, "clientUiMessages").IsNotNull();
      Condition.Requires(userProfile, "userProfile").IsNotNull();

      _settings = settings;
      _jobProvider = jobProviderFactory.GetPublishingJobProvider();
      _manifestProvider = manifestProviderFactory.GetManifestProvider();
      _databaseFactory = databaseFactory;
      _clientUiMessages = clientUiMessages;
      _language = userProfile.GetClientLanguage();
    }

    public void SetDatabaseContext(string databaseName)
    {
      Condition.Requires(databaseName, "databaseName").IsNotNullOrEmpty();

      _sourceDatabase = databaseName;
    }

    public async Task<PublishingJobEntity> GetActive()
    {
      var job = await _jobProvider.GetActive().ConfigureAwait(false);

      if (job != null)
      {
        var allLanguageCodes = job.Options.Languages;
        var allLanguages = GetLanguages(allLanguageCodes.ToArray());
        var allTargets = this.GetTargets();

        var manifestResults =
            await _manifestProvider.GetManifestStatuses(job.Manifests).ConfigureAwait(false);

        var jobEntity = ToEntity(job, manifestResults, allLanguages, allTargets);

        return jobEntity;
      }

      return null;
    }

    public async Task<IQueryable<PublishingJobEntity>> GetQueue(int skip, int take)
    {
      var jobs = await _jobProvider.GetQueue(skip, take).ConfigureAwait(false);

      var allLanguageCodes = jobs.SelectMany(j => j.Options.Languages);
      var allLanguages = GetLanguages(allLanguageCodes.ToArray());
      var allTargets = this.GetTargets();

      var manifestStatusTasks = new Dictionary<Guid, Task<ManifestStatus[]>>();

      foreach (var job in jobs)
      {
        manifestStatusTasks.Add(job.Id, _manifestProvider.GetManifestStatuses(job.Manifests));
      }

      await Task.WhenAll(manifestStatusTasks.Values).ConfigureAwait(false);

      var jobEntities = new List<PublishingJobEntity>();

      foreach (var statusTask in manifestStatusTasks)
      {
        var job = jobs.FirstOrDefault(j => j.Id == statusTask.Key);
        var manifestResults = statusTask.Value.Result;

        jobEntities.Add(ToEntity(job, manifestResults, allLanguages, allTargets));
      }

      return jobEntities.AsQueryable();
    }

    public async Task<IQueryable<PublishingJobEntity>> GetRecent(int skip, int take)
    {
      var jobs = await _jobProvider.GetRecent(skip, take).ConfigureAwait(false);

      var allLanguageCodes = jobs.SelectMany(j => j.Options.Languages);
      var allLanguages = GetLanguages(allLanguageCodes.ToArray());
      var allTargets = this.GetTargets();

      var manifestStatusTasks = new Dictionary<Guid, Task<ManifestStatus[]>>();

      foreach (var job in jobs)
      {
        manifestStatusTasks.Add(job.Id, _manifestProvider.GetManifestStatuses(job.Manifests));
      }

      await Task.WhenAll(manifestStatusTasks.Values).ConfigureAwait(false);

      var jobEntities = new List<PublishingJobEntity>();

      foreach (var statusTask in manifestStatusTasks)
      {
        var job = jobs.FirstOrDefault(j => j.Id == statusTask.Key);
        var manifestResults = statusTask.Value.Result;

        jobEntities.Add(ToEntity(job, manifestResults, allLanguages, allTargets));
      }

      return jobEntities.AsQueryable();
    }

    public void Add(PublishingJobEntity entity)
    {
      Condition.Requires(entity, "entity").IsNotNull();

      // Populate the rest of the Publish Job Entity properties not supplied by the UI.
      if (string.IsNullOrEmpty(entity.RequestedBy))
      {
        entity.RequestedBy = Context.User.Name;
      }

      entity.Type = entity.CalculateJobType(); // TODO: JDD - Should this be passed in the the UI somehow?
      entity.TimeRequested = DateUtil.GetShortIsoDateTime(DateTime.UtcNow);
      entity.Metadata = entity.Metadata ?? new Dictionary<string, string>();

      var publishJob = entity.ToJob(_databaseFactory);
      publishJob.Options.SetPublishType(entity.Type.DisplayName);
      publishJob.Options.SetDetectCloneSources(entity.Type == PublishJobType.SingleItem || entity.Type == PublishJobType.Incremental);

      var itemBucketsEnabled = _settings
          .GetBoolSetting(PublishingServiceConstants.Configuration.ItemBucketsEnabledSettingName, true);
      var bucketTemplateId = _settings
          .GetSetting(PublishingServiceConstants.Configuration.BucketTemplateIdSettingName);

      publishJob.Options.SetItemBucketsEnabled(itemBucketsEnabled);
      publishJob.Options.SetBucketTemplateId(new Guid(bucketTemplateId));

      Task.Run(async () =>
      {
        var job = await _jobProvider.Add(publishJob.Options).ConfigureAwait(false);

        // The Entity ID of the new Job must be set after it is generated to ensure that the response Location Header is correctly setup.
        entity.Id = job.Id.ToString("B");
      }).Wait();
    }

    public void Delete(PublishingJobEntity entity)
    {
      Condition.Requires(entity, "entity").IsNotNull();

      Guid jobId = new Guid(entity.Id);

      Task.Run(async () => { await _jobProvider.Delete(jobId).ConfigureAwait(false); }).Wait();
    }

    public bool Exists(PublishingJobEntity entity)
    {
      Condition.Requires(entity, "entity").IsNotNull();

      if (string.IsNullOrEmpty(entity.Id))
      {
        return false;
      }

      Guid jobId = new Guid(entity.Id);

      bool exists = false;
      Task.Run(async () => { exists = await _jobProvider.Exists(jobId).ConfigureAwait(false); }).Wait();

      return exists;
    }

    public PublishingJobEntity FindById(string id)
    {
      Condition.Requires(id, "id").IsNotNullOrEmpty();

      Guid jobId = new Guid(id);

      PublishJob job = null;
      Task.Run(async () => { job = await _jobProvider.Get(jobId).ConfigureAwait(false); }).Wait();

      var allLanguageCodes = job.Options.Languages;
      var allLanguages = GetLanguages(allLanguageCodes.ToArray());
      var allTargets = this.GetTargets();

      ManifestStatus[] manifestResults = null;
      Task.Run(
          async () =>
          {
            manifestResults = await _manifestProvider
                      .GetManifestStatuses(job.Manifests)
                      .ConfigureAwait(false);
          }).Wait();

      return ToEntity(job, manifestResults, allLanguages, allTargets);
    }

    public IQueryable<PublishingJobEntity> GetAll()
    {
      PublishJob[] jobs = Enumerable.Empty<PublishJob>().ToArray();

      Task.Run(async () => { jobs = await _jobProvider.GetAll().ConfigureAwait(false); }).Wait();

      var allLanguageCodes = jobs.SelectMany(j => j.Options.Languages);
      var allLanguages = GetLanguages(allLanguageCodes.ToArray());
      var allTargets = this.GetTargets();

      var jobEntities = new List<PublishingJobEntity>();

      foreach (var job in jobs)
      {
        ManifestStatus[] manifestResults = null;
        Task.Run(
            async () =>
            {
              manifestResults = await _manifestProvider
                          .GetManifestStatuses(job.Manifests)
                          .ConfigureAwait(false);
            }).Wait();

        jobEntities.Add(ToEntity(job, manifestResults, allLanguages, allTargets));
      }


      return jobEntities.AsQueryable();
    }

    public void Update(PublishingJobEntity entity)
    {
      Condition.Requires(entity, "entity").IsNotNull();

      PublishJob job = entity.ToJob(_databaseFactory);
      Task.Run(async () => { await _jobProvider.Update(job).ConfigureAwait(false); }).Wait();
    }

    private PublishingJobEntity ToEntity(PublishJob job, ManifestStatus[] jobManifestStatuses, Language[] allLanguages, Item[] allTargets)
    {
      var item = job.Options.ItemId.HasValue ? GetItem(new ID(job.Options.ItemId.Value)) : null;

      var publishJobType = job.CalculateJobType();

      var selectedLanguages = new List<PublishingLanguage>();

      foreach (var languageCode in job.Options.Languages)
      {
        var language = allLanguages.FirstOrDefault(l => l.Name == languageCode);

        if (language != null)
        {
          selectedLanguages.Add(new PublishingLanguage()
          {
            Code = languageCode,
            DisplayName = language.CultureInfo.DisplayName
          });
        }
      }

      var selectedTargets = new List<PublishingTarget>();

      foreach (var publishingTarget in job.Options.Targets)
      {
        var target = allTargets.FirstOrDefault(t => t.Name == publishingTarget);

        if (target != null)
        {
          var status = jobManifestStatuses
              .FirstOrDefault(s => s.TargetId.ToString("B") == target.ID.Guid.ToString("B"));

          selectedTargets.Add(new PublishingTarget()
          {
            Id = target.ID.ToString(),
            Name = target.DisplayName,
            Status = GetTargetStatusTranslation(job, status)
          });
        }
      }

      return new PublishingJobEntity
      {
        Id = job.Id.ToString("B"),
        ItemId = job.Options.ItemId.HasValue ? job.Options.ItemId.Value.ToString("B") : "",
        ItemName = item == null ? string.Empty : item.Name,
        StartPath = item == null ? string.Empty : item.Paths.ContentPath,
        ManifestIdList = job.Manifests,
        Languages = selectedLanguages,
        NumberOfItems = job.AffectedItems,
        RequestedBy = job.Options.User,
        Status = TranslateJobStatus(job.Status.ToPublishingJobStatus()),
        StatusMessage = job.StatusMessage == null ? string.Empty : job.StatusMessage,
        Targets = selectedTargets,
        TimeRequested = DateUtil.ToIsoDate(job.Queued),
        TimeStarted = job.Started.HasValue ? DateUtil.ToIsoDate(job.Started.Value) : null,
        TimeStopped = job.Stopped.HasValue ? DateUtil.ToIsoDate(job.Stopped.Value) : null,
        Type = publishJobType,
        TypeDisplayName = TranslatePublishType(publishJobType),
        IncludeDescendantItems = job.Options.Descendants,
        IncludeRelatedItems = job.Options.RelatedItems,
        SynchroniseWithTarget = true, // TODO: JDD - This functionality does not yet exist
        SourceDatabase = job.Options.Source,
        Metadata = job.Options.Metadata
      };
    }

    private string GetTargetStatusTranslation(PublishJob job, ManifestStatus status)
    {
      var targetStatus = TranslateManifestOperationStatus(ManifestOperationStatus.Error);

      if (status != null)
      {
        targetStatus = TranslateManifestOperationStatus(status.OperationStatus);
      }
      else if (job.Status == PublishJobStatus.Queued)
      {
        targetStatus = TranslateManifestOperationStatus(ManifestOperationStatus.Ready);
      }
      else if (job.Status == PublishJobStatus.Started)
      {
        targetStatus = TranslateManifestOperationStatus(ManifestOperationStatus.Running);
      }

      return targetStatus;
    }

    private string TranslatePublishType(PublishJobType publishingJobType)
    {
      var translatedLabel = String.Empty;

      if (publishingJobType == PublishJobType.Full)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.RepublishAllItemsText, "Text", _language);
      }
      else if (publishingJobType == PublishJobType.Incremental)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.SitePublishText, "Text", _language);
      }
      else if (publishingJobType == PublishJobType.SingleItem)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.ItemPublishText, "Text", _language);
      }
      else if (publishingJobType == PublishJobType.Republish)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.FullRepublishText, "Text", _language);
      }

      return translatedLabel;
    }

    private string TranslateJobStatus(PublishingJobStatus publishingJobStatus)
    {
      var translatedLabel = String.Empty;

      if (publishingJobStatus == PublishingJobStatus.Cancelled)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.RepublishAllItemsText, "Text", _language);
      }
      else if (publishingJobStatus == PublishingJobStatus.Complete)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.CompleteStatusText, "Text", _language);
      }
      else if (publishingJobStatus == PublishingJobStatus.CompleteWithErrors)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.ItemPublishText, "Text", _language);
      }
      else if (publishingJobStatus == PublishingJobStatus.Failed)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.FailedStatusText, "Text", _language);
      }
      else if (publishingJobStatus == PublishingJobStatus.Started)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.InProgressStatusText, "Text", _language);
      }
      else if (publishingJobStatus == PublishingJobStatus.Queued)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.QueuedStatusText, "Text", _language);
      }

      return translatedLabel;
    }

    private string TranslateManifestOperationStatus(ManifestOperationStatus manifestOperationStatus)
    {
      var translatedLabel = String.Empty;

      if (manifestOperationStatus == ManifestOperationStatus.Complete)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.CompleteStatusText, "Text", _language);
      }
      else if (manifestOperationStatus == ManifestOperationStatus.Error)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.FailedStatusText, "Text", _language);
      }
      else if (manifestOperationStatus == ManifestOperationStatus.Ready)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.QueuedStatusText, "Text", _language);
      }
      else if (manifestOperationStatus == ManifestOperationStatus.Running)
      {
        translatedLabel = _clientUiMessages.GetMessageText(ConfigurationConstants.RunningStatusText, "Text", _language);
      }

      return translatedLabel;
    }

    private Item GetItem(ID itemId)
    {
      if (_sourceDatabase == null)
      {
        _sourceDatabase = _settings.DefaultSourceDatabaseName;
      }

      var database = _databaseFactory.GetDatabase(_sourceDatabase);

      if (database == null)
      {
        return null;
      }

      return database.GetItem(itemId, _language);
    }

    private Language[] GetLanguages(string[] languageCodes)
    {
      if (!languageCodes.Any())
      {
        return Enumerable.Empty<Language>().ToArray();
      }

      var database = _databaseFactory.GetDatabase(_settings.DefaultSourceDatabaseName).Database;

      return LanguageManagerExtensions.GetLanguagesByCode(languageCodes, database);
    }

    private Item[] GetTargets()
    {
      var database = _databaseFactory.GetDatabase(_settings.DefaultSourceDatabaseName).Database;
      var targets = PublishManager.GetPublishingTargets(database);

      return targets.ToArray();
    }
  }
}