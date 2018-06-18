-- Databricks notebook source
CREATE DATABASE  IF NOT EXISTS dim;
CREATE DATABASE  IF NOT EXISTS rachinth;


-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimLeadUnifiedActivityLogTxt;
CREATE EXTERNAL TABLE rachinth.DimLeadUnifiedActivityLogTxt(
LeadID bigint 
,LeadActivityID bigint 
,DataSourceID int 
,DataSourceName string 
,ActivityTypeID int 
,ActivityTypeName string 
,ProgramID bigint 
,ProgramName string 
,AllocadiaId bigint 
,DimDemandGenChannelSKID bigint 
,Channel string 
,Vehicle string 
,Tactic string 
,WebSessionID string 
,ActivityDateTime timestamp 
,ExternalSourceId string 
,QueryStringSource string 
,PageUrl string 
,ReferType string 
,RawQueryString string 
,CampaignId string 
,Product string 
,ProgramType string 
,ProgramChannel string 
,GEP string 
,BusinessGroup string 
,IsSuccess boolean 
,NewStatusID bigint 
,NewStatus string 
,OldStatusID bigint 
,OldStatus string 
,VideoName string 
,EmbedUrl string 
,VideoLength string 
,PlayerName string 
,CustomAttributes string 
,PlayerId string 
,PercentViewed string 
,VideoId string 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
,UseForAttribution int 
,BusinessGroup_Lower string 
,MPAAIndividualId bigint 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimLeadUnifiedActivityLogUAT'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimLeadUnifiedActivityLogDBParqt;
CREATE TABLE dim.DimLeadUnifiedActivityLogDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimLeadUnifiedActivityLogTxt;

-- COMMAND ----------

select count(LeadID) from dim.DimLeadUnifiedActivityLogDBParqt;

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimLeadTxt;
CREATE EXTERNAL TABLE rachinth.DimLeadTxt(
LeadSKID bigint 
,NativeLeadID String 
,NativeMarketoLeadID bigint 
,OwnerTeamSKID bigint 
,CRMSiteSKID bigint 
,CRMCampaignSKID bigint 
,AccountSKID bigint 
,OwnerSystemUserSKID bigint 
,CreatedByRepresentativeSKID bigint 
,OwnerRepresentativeSKID bigint 
,FirstTouchCreatorRepresentativeSKID bigint 
,OriginalEngagerRepresentativeSKID bigint 
,OriginalOwnerRepresentativeSKID bigint 
,TALCreatorRepresentativeSKID bigint 
,SALCreatorRepresentativeSKID bigint 
,TQLCreatorRepresentativeSKID bigint 
,OriginalLeadOwningTeamSKID bigint 
,BusinessGroupSKID int 
,CountrySKID int 
,CountryName string 
,SalesStageDateSKID int 
,CreatedDateSKID int 
,ModifiedDateSKID int 
,EstimatedCloseDateSKID int 
,MAQLDateSKID int 
,TALDateSKID int 
,ReachedDateSKID int 
,TQLDateSKID int 
,SALDateSKID int 
,WorkedDateSKID int 
,FirstPhoneCallDateSKID int 
,DataSourceSKID int 
,MarketoLeadDataSourceSKID int 
,WWLMResponseID string 
,SubscriptionID string 
,LeadTenantID string 
,DataSourceName string 
,LeadNumber string 
,LeadSource string 
,SourceSubType string 
,LeadRating string 
,PrimaryProductRevSumDivision string 
,ProductSKU string 
,SalesStage string 
,SalesStageStartDate timestamp 
,Priority string 
,StatusReason string 
,Status string 
,CreatedDate timestamp 
,FirstPhoneCallDate timestamp 
,ModifiedDate timestamp 
,EstimatedCloseDate timestamp 
,MAQLDate timestamp 
,TALDate timestamp 
,MAQL_System_Date timestamp 
,TAL_System_Date timestamp 
,ReachedDate timestamp 
,TQLDate timestamp 
,SALDate timestamp 
,WorkedDate timestamp 
,LeadType string 
,MarketingSource string 
,LeadScore decimal 
,LeadPromoCode string 
,LeadPriorityGroup string 
,Description string 
,TALFirstContactAging bigint 
,TALFirstContactAgingBand string 
,LeadBusinessUnitSKID bigint 
,WorkedBy string 
,IsLeadConvertedToOpportunity boolean 
,IsLeadSourceValid boolean 
,IsMALSegmentValid boolean 
,IsLeadSalesStageValid boolean 
,IsLeadStatusValid boolean 
,IsLeadCreatedDateValid boolean 
,IsLeadCriteriaMet boolean 
,IsMQL boolean 
,IsMAQL boolean 
,IsTAL boolean 
,IsReached boolean 
,IsTQL boolean 
,IsSAL boolean 
,IsWorked boolean 
,IsCSS boolean 
,IsIOE boolean 
,IsSDS boolean 
,IsTALProcessCompliantKey boolean 
,IsTQLProcessCompliantKey boolean 
,IsSALProcessCompliantKey boolean 
,IsMAQLProcessCompliantKey boolean 
,CSS_BG string 
,ProgramType string 
,IsActive boolean 
,ODSStatus int 
,InitializationVector string 
,CompanyName string 
,TerritorySalesDistrict string 
,TerritorySalesSubDistrict string 
,EndCustomerSegmentName string 
,IsPendingMAQL boolean 
,IsWorkedMAQL boolean 
,IsWorkedAndTALConverted boolean 
,IsWorkedAndDisqualified boolean 
,IsWorkedAndInProgress boolean 
,IsMAQLAccepted boolean 
,IsMAQLExpired boolean 
,IsMAQLRejected boolean 
,IsTALConverted boolean 
,IsTALDisqualified boolean 
,IsTQLAccepted boolean 
,IsTQLRejected boolean 
,IsMAQLAcceptedAndConverted boolean 
,IsMAQLAcceptedAndInProgress boolean 
,IsMAQLAcceptedAndRecycled boolean 
,IsMAQLAcceptedAndRejected boolean 
,IsTALPending boolean 
,IsTALRecycled boolean 
,IsTQLOpen boolean 
,PreferredLanguage string 
,OwningTeamName string 
,PrioritizationScore decimal 
,CRMEntityId string 
,ContactCRMEntityID string 
,SegmentSKID bigint 
,LeadScoreGroup string 
,UpdatedDateSKID int 
,UpdatedDate timestamp 
,CallStatus string 
,State string 
,IsTeleAssisted boolean 
,SQLId string 
,IsSQL boolean 
,SQLDate timestamp 
,PrimaryProduct string 
,BGDataSource string 
,DimMarketoLeadSKID bigint 
,DMCOpportunityID String 
,IsMarketoCE boolean 
,IsWin boolean 
,LeadCreatedDate string 
,LeadCreatedDateFiscalMonth string 
,LeadCreatedDateFiscalMonthID int 
,LeadCreatedDateFiscalQuarter string 
,LeadCreatedDateFiscalWeek string 
,LeadCreatedDateFiscalYear string 
,LeadCreatedDateTime timestamp 
,LeadEstimatedClosedDate string 
,LeadEstimatedClosedDateFiscalMonth string 
,LeadEstimatedClosedDateFiscalMonthID int 
,LeadEstimatedClosedDateFiscalQuarter string 
,LeadEstimatedClosedDateFiscalWeek string 
,LeadEstimatedClosedDateFiscalYear string 
,LeadModifiedDate string 
,LeadModifiedDateFiscalMonth string 
,LeadModifiedDateFiscalMonthID int 
,LeadModifiedDateFiscalQuarter string 
,LeadModifiedDateFiscalWeek string 
,LeadModifiedDateFiscalYear string 
,LifeCycleStage string 
,Product string 
,SALDateFiscalMonth string 
,SALDateFiscalMonthID int 
,SALDateFiscalQuarter string 
,SALDateFiscalWeek string 
,SALDateFiscalYear string 
,SALDateTime timestamp 
,TALDateFiscalMonth string 
,TALDateFiscalMonthID int 
,TALDateFiscalQuarter string 
,TALDateFiscalWeek string 
,TALDateFiscalYear string 
,TALDateTime timestamp 
,TQLDateFiscalMonth string 
,TQLDateFiscalMonthID int 
,TQLDateFiscalQuarter string 
,TQLDateFiscalWeek string 
,TQLDateFiscalYear string 
,TQLDateTime timestamp 
,LeadOwnerFirstName string 
,LeadOwnerMiddleName string 
,LeadOwnerLastName string 
,LeadOwnerInitializationVector string 
,LeadParentAccountId String 
,LeadDisposition string 
,LeadOwningTeam string 
,LeadOriginalEngagerID String 
,LeadRCode string 
,SALCreatorID String 
,TALCreatorID String 
,TQLCreatorID String 
,LeadScoreBand string 
,FirstTouchCreatorID String 
,LeadSystemuserIsWWISRep boolean 
,LeadAge int 
,LeadSalesStageAge int 
,MAQLNoContactAging bigint 
,LeadAgeGroup string 
,LeadSalesStageAgeGroup string 
,MAQLNoContactAgingBand string 
,AcceleratedLeadFlag boolean 
,IsInbound boolean 
,OriginatingInsideSalesCenter string 
,OriginalLeadOwningTeam string 
,LeadOriginalOwnerID String 
,LastLeadOwningTeam string 
,LeadTypePipeline string 
,EngagementId String 
,stampstatuschange timestamp 
,ModifiedById String 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
,DeltaDateTime timestamp 
,TaskExecutionId int 
,IncidentId string 
,IncidentTimeStamp timestamp 
,CurrencyId String 
,EstimatedQuantity int 
,EstimatedValue decimal 
,ProductSKID bigint 
,Subject string 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimLead'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimLeadDBParqt;
CREATE TABLE dim.DimLeadDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimLeadTxt;

-- COMMAND ----------

select count(LeadSKID) from dim.DimLeadDBParqt;

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimCRMCampaignTxt;
CREATE EXTERNAL TABLE rachinth.DimCRMCampaignTxt(
CRMCampaignSKID bigint 
,NativeCRMCampaignID String 
,CreatedBySystemUserSKID bigint 
,OwnerSystemUserSKID bigint 
,CreatedDateSKID int 
,ModifiedDateSKID int 
,TacticStartDateSKID int 
,TacticEndDateSKID int 
,MarketingVehicleSKID bigint 
,MarketingTacticSKID bigint 
,DataSourceSKID int 
,TacticName string 
,Origin string 
,PromotionCode string 
,Status string 
,StatusReason string 
,DataSourceName string 
,CreatedDate timestamp 
,ModifiedDate timestamp 
,TacticStartDate timestamp 
,TacticEndDate timestamp 
,IsActive boolean 
,GlobalEngagementProgram string 
,GlobalEngagementProgramScenario string 
,LastTouchedProgramID int 
,LastTouchedProgramName_FriendlyName string 
,LastTouchedProgramName_Name_o_Matic string 
,InitializationVector string 
,SourceCampaignOwnerFirstName string 
,SourceCampaignOwnerMiddleName string 
,SourceCampaignOwnerLastName string 
,SourceCampaignOwnerName_InitializationVector string 
,IncidentId string 
,IncidentTimeStamp timestamp 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimCRMCampaign'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimCRMCampaignDBParqt;
CREATE TABLE dim.DimCRMCampaignDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimCRMCampaignTxt;

-- COMMAND ----------

select count(*) from dim.DimCRMCampaignDBParqt

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimMarketoLeadTxt;
CREATE EXTERNAL TABLE rachinth.DimMarketoLeadTxt(
DimMarketoLeadSKID bigint 
,MarketoLeadID bigint 
,DataSourceSKID int 
,DataSourceName string 
,AAIOTLeadScore int 
,AAIOTLeadScoreGroup string 
,AAIOTLeadScoreSort int 
,AAIOTproductInterestFirstDate timestamp 
,AAIOTproductInterestFirstDateSKID bigint 
,AAIOTproductInterestLatestDate timestamp 
,AAIOTproductInterestLatestDateSKID bigint 
,AAIOT_Interest boolean 
,BusinessGroupCE boolean 
,BusinessGroupOffice boolean 
,BusinessGroupWindows boolean 
,EnrichmentDate timestamp 
,MatchConfidence string 
,MatchType string 
,AAIOTEmailContactable boolean 
,AzureEmailContactable int 
,AzureLeadScore int 
,AzureLeadScoreGroup string 
,AzureLeadScoreSort int 
,AzureproductinterestfirstdateSKID bigint 
,Azureproductinterestfirstdate timestamp 
,AzureproductinterestlatestdateSKID bigint 
,azureproductinterestlatestdate timestamp 
,AzureInterest boolean 
,CRMEmailContactable boolean 
,CRMLeadScore int 
,CRMLeadScoreGroup string 
,CRMLeadScoreSort int 
,CRMProductInterestFirstDateSKID bigint 
,CRMProductInterestFirstDate timestamp 
,CRMProductInterestLatestDateSKID bigint 
,CRMProductInterestLatestDate timestamp 
,CRMInterest boolean 
,EMSEmailContactable boolean 
,EMSLeadScore int 
,EMSLeadScoreGroup string 
,EMSLeadScoreSort int 
,EMSProductInterestFirstDateSKID bigint 
,EMSProductInterestFirstDate timestamp 
,EMSProductInterestLatestDateSKID bigint 
,EMSProductInterestLatestDate timestamp 
,EMSInterest boolean 
,LeadSourceDetails string 
,MicrosoftGeneralEmailContactable boolean 
,MicrosoftPromotionalPhonePreference string 
,MSWidePromotionalUse string 
,OMSEmailContactable boolean 
,OriginalLeadSource string 
,AnyEmailContactable boolean 
,OriginalLeadSourceDetails string 
,PowerBIEmailContactable boolean 
,PowerBILeadScore int 
,PowerBILeadScoreGroup string 
,PowerBILeadScoreSort int 
,PowerBIProductInterestFirstDateSKID bigint 
,PowerBIProductInterestFirstDate timestamp 
,PowerBIProductInterestLatestDateSKID bigint 
,PowerBIProductInterestLatestDate timestamp 
,PowerBIInterest boolean 
,ProductInterestFirst string 
,SQLEmailContactable boolean 
,VSOEmailContactable boolean 
,VSOLeadScore int 
,VSOLeadScoreGroup string 
,VSOLeadScoreSort int 
,VSOProductInterestFirstDateSKID bigint 
,VSOProductInterestFirstDate timestamp 
,VSOProductInterestLatestDateSKID bigint 
,VSOProductInterestLatestDate timestamp 
,VSOInterest boolean 
,cookies string 
,Email string 
,employeeRange string 
,IsDeleted boolean 
,CompanySize string 
,lSCompany string 
,LSCompanySize string 
,LSCompanySizeRange string 
,LSAzurePredictiveScore int 
,LSAzurePredictiveScoreGroup string 
,LSLeadScoreDeveloper int 
,LSLeadScoreDeveloperGroup string 
,LSLeadScoreFinance int 
,LSLeadScoreFinanceGroup string 
,LSLeadScoreITDecisionMaker int 
,LSLeadScoreITDecisionMakerGroup string 
,LSLeadScoreITImplementer int 
,LSLeadScoreITImplementerGroup string 
,LSLeadScoreMarketing int 
,LSLeadScoreMarketingGroup string 
,LSLeadScoreOperations int 
,LSLeadScoreOperationsGroup string 
,lSProfilePowerBIAnna int 
,lSProfilePowerBIAnnaGroup string 
,lSProfilePowerBINancyInteger int 
,lSProfilePowerBINancyIntegerGroup string 
,LSStatus string 
,lSTPID string 
,MDID string 
,MSFTLeadScore int 
,MSFTLeadScoreGroup string 
,MSFTLeadScoreSort int 
,NumberOfEmployees int 
,OMSLeadScore int 
,OMSLeadScoreGroup string 
,OMSLeadScoreSort int 
,OMSProductInterestFirstDateSKID bigint 
,oMSProductInterestFirstDate timestamp 
,OMSProductInterestLatestDateSKID bigint 
,oMSProductInterestLatestDate timestamp 
,OMSInterest boolean 
,RM_DM_AzureAcquisition_Stage string 
,RM_EA_AzureAcquisition_Stage string 
,SQLLeadScore int 
,SQLLeadScoreGroup string 
,SQLLeadScoreSort int 
,SQLProductInterestFirstDate timestamp 
,SQLProductInterestLatestDate timestamp 
,SQLInterest boolean 
,JobTitle string 
,MarketoOfficeId bigint 
,OfficeBehaviorScore int 
,OfficeDemographicScore int 
,OfficeLeadScore int 
,OfficeTrialUsageScore int 
,PrimaryLSProfile string 
,IsSuppressedLead boolean 
,IsValidEmail boolean 
,IsNurturable boolean 
,IsSellable boolean 
,acquisitionProgramId string 
,InitializationVector string 
,emailHash bigint 
,emailDomain string 
,AccountName string 
,AccountTPID string 
,createdAt string 
,updatedAt string 
,Vertical string 
,TPID string 
,SubSegment string 
,Segment string 
,originalSourceType string 
,OfficeBehaviorScoreGroup string 
,OfficeDemographicScoreGroup string 
,OfficeLeadScoreGroup string 
,OfficeTrialUsageScoreGroup string 
,LeadSource string 
,LeadScore bigint 
,LeadScoreGroup string 
,CRMAccountId string 
,Industry string 
,CurrentWeb2Marketo string 
,company string 
,TopicofInterest string 
,JobRole string 
,LeadCompany string 
,BusinessGroupOfficeDate timestamp 
,BusinessGroupOfficeFiscalWeekID int 
,BusinessGroupOfficeFiscalWeek int 
,BusinessGroupOfficeFiscalMonth string 
,BusinessGroupOfficeFiscalMonthID int 
,BusinessGroupOfficeFiscalQuarter string 
,BusinessGroupOfficeFiscalYear string 
,BusinessGroupCEDate timestamp 
,BusinessGroupCEFiscalWeekID int 
,BusinessGroupCEFiscalWeek int 
,BusinessGroupCEFiscalMonth string 
,BusinessGroupCEFiscalMonthID int 
,BusinessGroupCEFiscalQuarter string 
,BusinessGroupCEFiscalYear string 
,BusinessGroupWindowsDate timestamp 
,BusinessGroupWindowsFiscalWeekID int 
,BusinessGroupWindowsFiscalWeek int 
,BusinessGroupWindowsFiscalMonth string 
,BusinessGroupWindowsFiscalMonthID int 
,BusinessGroupWindowsFiscalQuarter string 
,BusinessGroupWindowsFiscalYear string 
,CountrySKID int 
,mIPSalesMotion string 
,IncidentId string 
,IncidentTimeStamp timestamp 
,cPMAABDEmailPreferencePremiumContent string 
,cPMArtificialIntelligenceEmailPreferenceNewsletter string 
,cPMMicrosoftOnBehalfofPartnerEmailPreferenceUpdated string 
,cPMAAIoTEmailPreferenceNewsletter string 
,cPMArtificialIntelligenceEmailPreferencePremiumContent string 
,cPMISVRCTEmailPreferencePremiumContent string 
,cPMMOSEmailPreferencePremiumContent string 
,cPMOfficeEmailPreferencePremiumContent string 
,cPMPartnerPromotionalEmailPreference string 
,cPMSurfaceEmailPreferencePremiumContent string 
,cPMWNDWSEmailPreferencePremiumContent string 
,cPMAzureEmailPreferenceNewsletter string 
,cPMEMSEmailPreferenceNewsletter string 
,cPMPowerBIEmailPreferenceNewsletter string 
,cPMVSOEmailPreferenceNewsletter string 
,cPMUSEmailPreferenceCommercialNewsletter string 
,cPMUSEmailPreferenceEducationNewsletter string 
,cPMUSEmailPreferenceGovernmentNewsletter string 
,cPMUSEmailPreferenceHealthcareNewsletter string 
,cPMUSEmailPreferenceSMBBusinessInsightsNews string 
,cPMEducationEmailPreferencePremiumContent string 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
,DeltaDateTime timestamp 
,TaskExecutionId int 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimMarketoLead'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimMarketoLeadDBParqt;
CREATE TABLE dim.DimMarketoLeadDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimMarketoLeadTxt;

-- COMMAND ----------

select count(*) from dim.DimMarketoLeadDBParqt;

-- COMMAND ----------

CREATE EXTERNAL TABLE rachinth.DimAttributionProductTxt(
ProductSKID bigint 
,ProductName string 
,ProductFamilyCode string 
,ProductFamily string 
,RevSumCategory string 
,RevSumDivision string 
,SuperRevSumDivision string 
,ProductTypeCode int 
,ProductType string 
,IsCloudProduct boolean 
,IsActive boolean 
,BusinessName string 
,BusinessUnitName string 
,ProductDivisionName string 
,SuperDivisionName string 
,ProductUnitName string 
,ProductRollUpName string 
,ItemGroup string 
,ItemName string 
,Manufacturer string 
,Model string 
,PrimaryProduct string 
,ProductNumber string 
,PriceListType string 
,ODSStatus int 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimAttributionProduct'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimAttributionProductDBParqt;
CREATE TABLE dim.DimAttributionProductDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimAttributionProductTxt;

-- COMMAND ----------


select count(*) from dim.DimAttributionProductDBParqt


-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimAccountTxt;
CREATE EXTERNAL TABLE rachinth.DimAccountTxt(
AccountSKID bigint 
,NativeAccountID String 
,AccountName string 
,AccountNumber string 
,OwnerId String 
,TerritorySKID bigint 
,CreatedDate timestamp 
,CreatedDateSKID int 
,DataSourceSKID int 
,DataSourceName string 
,CountrySKID int 
,State string 
,City string 
,MSSalesTopParentID string 
,ParentingLevel string 
,MSSalesId string 
,ParentAccountID String 
,TPName string 
,EndCustomerSegmentSKID bigint 
,EndCustomerSegmentName string 
,SegmentGrouping string 
,EndCustomerSubSegmentName string 
,Industry string 
,IndustryVertical string 
,MALSegmentSKID bigint 
,MALSegmentName string 
,MALSubSegmentName string 
,MALCreditedDistrictID int 
,MALCreditedDistrictName string 
,MALCreditedSubDistrictID int 
,MALCreditedSubDistrictName string 
,MALIndustry string 
,MALVertical string 
,MALVerticalCategory string 
,MALSector string 
,ATUManager string 
,ATUName string 
,ATUGrouping string 
,SalesTerritory string 
,CreditedRegion string 
,CreditedDistrict string 
,SectorType string 
,Status string 
,IsActive boolean 
,Country string 
,SalesGroup string 
,Subsidiary string 
,SalesUnit string 
,SalesUnitGrouping string 
,OEMManaged boolean 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
,IncidentId string 
,IncidentTimeStamp timestamp 
,OEMOrganizationTypeCode int 
,OEMSegmentTypeCode int 
,oemorganizationtype string 
,oemsegmenttype string 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimAccount'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimAccountDBParqt;
CREATE TABLE dim.DimAccountDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimAccountTxt;

-- COMMAND ----------

select count(*) from dim.DimAccountDBParqt

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimDemandGenChannelTxt;
CREATE EXTERNAL TABLE rachinth.DimDemandGenChannelTxt(
DimDemandGenChannelSKID bigint 
,Channel string 
,Vehicle string 
,Tactic string 
,SubTactic string 
,Description string 
,CampaignID string 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimDemandGenChannel'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimDemandGenChannelDBParqt;
CREATE TABLE dim.DimDemandGenChannelDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimDemandGenChannelTxt;

-- COMMAND ----------

select count(*) from dim.DimDemandGenChannelDBParqt

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimAttributionGeographyTxt;
CREATE EXTERNAL TABLE rachinth.DimAttributionGeographyTxt(
Area string 
,Country string 
,SubRegionID int 
,SubRegion string 
,RegionID int 
,Region string 
,Subsidiary string 
,SubsidaryID int 
,DimGeographySKID int 
,DefaultSubsidiaryCountry int 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimAttributionGeography'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimAttributionGeographyDBParqt;
CREATE TABLE dim.DimAttributionGeographyDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimAttributionGeographyTxt

-- COMMAND ----------

select count(*) from dim.DimAttributionGeographyDBParqt;

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimProgramTxt;
CREATE EXTERNAL TABLE rachinth.DimProgramTxt(
ProgramSKID bigint 
,Campaign string 
,NativeProgramID bigint 
,ProgramName string 
,Channel string 
,ProgramCountry string 
,ProgramArea string 
,Type string 
,UnifiedProductSKID bigint 
,Product string 
,CallProduct string 
,DataSourceSKID int 
,DataSourceName string 
,CountrySKID bigint 
,AreaSKID bigint 
,StartDateSKID bigint 
,StartDate timestamp 
,EndDateSKID bigint 
,EndDate timestamp 
,FriendlyProgramName string 
,FieldVsCorp string 
,GlobalEngagementProgramScenario string 
,NonGEPCampaign string 
,WorkspaceName string 
,ProgramOwner string 
,ProgramOwnerPIIHash bigint 
,ProgramOwnerIV string 
,GEP string 
,IsMultiTouchAttributed int 
,ConversationStage int 
,ProgramAllocadiaID string 
,IncidentId string 
,IncidentTimeStamp timestamp 
,BusinessGroup string 
,createdAt string 
,updatedAt string 
,DeltaDateTime timestamp 
,TaskExecutionId int 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
,IsActive boolean 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimProgram'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimProgramDBParqt;
CREATE TABLE dim.DimProgramDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimProgramTxt

-- COMMAND ----------

DROP TABLE IF EXISTS rachinth.DimUnifiedProductTxt;
CREATE EXTERNAL TABLE rachinth.DimUnifiedProductTxt(
UnifiedProductSKID bigint 
,ProductName string 
,DataSourceSKID int 
,DataSourceName string 
,CallProduct string 
,BusinessGroup string 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimUnifiedProduct'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimUnifiedProductDBParqt;
CREATE TABLE dim.DimUnifiedProductDBParqt USING PARQUET 
 AS SELECT * FROM rachinth.DimUnifiedProductTxt

-- COMMAND ----------

CREATE EXTERNAL TABLE rachinth.DimOpportunityTxt(
OpportunitySKID bigint 
,NativeOpportunityID String 
,ActualCloseDateSKID int 
,LeadSKID bigint 
,LicensingProgramSKID bigint 
,CreatedDateSKID int 
,ModifiedDateSKID int 
,EstimatedCloseDateSKID int 
,AccountSKID bigint 
,CurrentSalesStageModifiedDateSKID int 
,SQODateSKID int 
,WinDateSKID int 
,WinDate_LegacySKID int 
,OwnerTeamSKID bigint 
,CRMSiteSKID bigint 
,CRMCampaignSKID bigint 
,OwnerSystemUserSKID bigint 
,CreatedBySystemUserSKID bigint 
,OwnerRepresentativeSKID bigint 
,CreatedByRepresentativeSKID bigint 
,OpportunityBusinessUnitSKID bigint 
,OriginalOpportunityCRMSiteSKID bigint 
,OriginalOpportunityOwningRepresentativeSKID bigint 
,OriginalOpportunityOwningTeamSKID bigint 
,SQOCreatorRepresentativeSKID bigint 
,WinCreatorRepresentativeSKID bigint 
,DataSourceSKID int 
,DataSourceName string 
,LicensingProgram string 
,LicensingSubType string 
,OpportunityNumber string 
,OpportunitySource string 
,ProductSKU string 
,Topic string 
,Priority string 
,CurrentSalesStageModifiedOn timestamp 
,ActualCloseDate timestamp 
,CreatedDate timestamp 
,ModifiedDate timestamp 
,SQODate timestamp 
,WinDate timestamp 
,WinDate_Legacy timestamp 
,EstimatedCloseDate timestamp 
,OriginatingLeadId String 
,PrimaryPartnerAccountId String 
,SendToPartner boolean 
,SourceSubType string 
,Status string 
,OpportunityState string 
,SalesStageProbabilityName string 
,OpportunityRatingCode int 
,StateCode int 
,StatusReason string 
,ForecastComments string 
,Opportunity_InitializationVector string 
,OpportunityAge int 
,Recommendation string 
,SalesStage string 
,ModifiedById String 
,SubscriptionID string 
,OpportunityTenantID string 
,OpportunitySalesStageAge int 
,OpportunitySalesStageAgeGroup string 
,OpportunityAgeGroup string 
,OpportunityPromoCode string 
,RoutedToPartner string 
,FlowRouting string 
,PartnerNumber string 
,PrimaryPartnerName string 
,PartnerAccountSKID bigint 
,SeatBand string 
,PipelineRevenueGrouping string 
,DaysToOpptyLost bigint 
,DaysToOpptyWin bigint 
,RCodeGeneratedBy string 
,AccountName string 
,MSSalesTopParentID string 
,WorkedBy string 
,CSS_BG string 
,ProgramType string 
,OpportunityLicensingCategory string 
,CustomSalesStatusGrouping string 
,IsOpportunitySourceValid boolean 
,IsConvertedFromLead boolean 
,IsOpportunityEstimatedCloseDateValid boolean 
,IsOpportunityStatusValid boolean 
,IsMALSegmentValid boolean 
,IsOpportunityClosedInCurrentFiscalYear boolean 
,IsOpportunityCriteriaMet boolean 
,IsLicensingSubTypeValid boolean 
,IsWinCriteriaMet boolean 
,IsSQO boolean 
,IsWin boolean 
,IsLost boolean 
,IsRevenueGreaterThan50K boolean 
,IsMPSCRouted boolean 
,IsMSXC2PC boolean 
,IsPartnerRouting boolean 
,HasCompetitor boolean 
,IntId string 
,IsSQOProcessCompliantKey boolean 
,IsWinProcessCompliantKey boolean 
,IsCSS boolean 
,IsOpptyWin boolean 
,IsOpptyLost boolean 
,IsOpptyClose boolean 
,IsPartnerDealAssociated boolean 
,IsPastEstimatedClose boolean 
,IsExcludedOpportunity boolean 
,OpportunityType int 
,Closeprobability int 
,FieldOwner string 
,AzureCRM_InitializationVector string 
,IsSAL boolean 
,SALDateSKID int 
,SALDate timestamp 
,IsSQL boolean 
,SQLDateSKID int 
,SQLDate timestamp 
,MSXOpportunityIDCheckSum string 
,LifeCycleStage string 
,IsOfficeOpportunity int 
,IsActive boolean 
,IsZeroProductEntry boolean 
,SALId String 
,SQLId String 
,WinId String 
,Firstname string 
,Lastname string 
,CompanyName string 
,EmailAddress string 
,LeadProductFamilyName string 
,IsLeadOriginated boolean 
,OriginatingLeadNumber string 
,OpportunityBusinessUnit string 
,IsMAQLClose boolean 
,OpportunitySeats bigint 
,WinCreatorID String 
,OpportunityOwningTeam string 
,OriginalOpportunityOwningTeam string 
,Owner_InitializationVector string 
,OriginalOpportunitySite string 
,SQOCreatorID String 
,OpportunityEstimatedRevenueGrouping string 
,PartnermotionSeqNo bigint 
,IsOrderComplete boolean 
,LeadTypePipeline string 
,IncidentId string 
,IncidentTimeStamp timestamp 
,OwnerId String 
,Actualvalue decimal 
,CurrencyId String 
,EstimatedValue decimal 
,ParentOpportunityId String 
,ODSStatus int 
,DWStartDate timestamp 
,DWEndDate timestamp 
,DWUpdatedDateTime timestamp 
)
location 'dbfs:/mnt/CDPvNextProd/rachinth/DimOpportunity'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|'
STORED AS TEXTFILE;

DROP TABLE IF EXISTS dim.DimOpportunityDBParqt;
CREATE TABLE dim.DimOpportunityDBParqt USING PARQUET 
 AS SELECT * FROM dim.DimOpportunityTxt