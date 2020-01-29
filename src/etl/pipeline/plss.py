import pyreadstat
import datetime
import os
import futil
import sqlalchemy as sa
from sqlalchemy import text
import data_file

class Plss_file(data_file.Data_file):

    def __init__(self, settings):
        self.db = settings['db']
        self.filename = settings['plss']['file']

    def read(self):
        engine = sa.create_engine( self.db)
        conn = engine.connect()
        print( '----  begin >' + str(datetime.datetime.now()))
        print('-----  read >' + str(datetime.datetime.now()))
        df, meta = pyreadstat.read_dta( self.filename)
        df.to_sql("plss_tmp", con=engine, if_exists="replace")
        print('----  merge >' + str(datetime.datetime.now()))
        t = text( """
                      insert into plss ( CourseParticipationId,CourseCalendarId,ApplicationId,DateStartCourse,DateActualFinishCourse,
                                         OutcomeId,DateDeleted,OutcomeStatusId,OutcomeCertificationId,OutcomeCertificationAwardId,
                                         OutcomeEarlyFinishReasonId,OriginalOutcomeId,ApplicationOriginId,ApplicationStatusId,FETProviderId,
                                         DeliveryFETProviderId,ProgrammeId,TargetAwardId,PublishedCourseTitle,ProgrammeCategoryId,
                                         DateActualStart,DateActualFinish,DeliveryModeId,Title,Cluster,ProgrammeAwardLevel,
                                         ProgrammeCertified,TargetAward,AwardAchievable,AwardSummary,ISCEDBroadUID,ISCEDDetailedFieldID,
                                         learnerfinishdate,learnerstartyear,finishmonth,finishyear,Gender,ParentFETProviderId,Parent,
                                         ParentDivisionProviderId,FETProviderTypeId,FETProviderName,IsETB,CountyId,
                                         programmecategorydescription,hasemployers,LearnerCountyId,NationalityId,outcomestatusdescription,
                                         outcomecertificationdescription,isoutcomecertified,outcomecertificationawarddescrip,
                                         outcomedescription,hash_ppsn,age )
                          select te.CourseParticipationId,te.CourseCalendarId,te.ApplicationId,te.DateStartCourse,te.DateActualFinishCourse,
                                 te.OutcomeId,te.DateDeleted,te.OutcomeStatusId,te.OutcomeCertificationId,te.OutcomeCertificationAwardId,
                                 te.OutcomeEarlyFinishReasonId,te.OriginalOutcomeId,te.ApplicationOriginId,te.ApplicationStatusId,te.FETProviderId,
                                 te.DeliveryFETProviderId,te.ProgrammeId,te.TargetAwardId,te.PublishedCourseTitle,te.ProgrammeCategoryId,
                                 te.DateActualStart,te.DateActualFinish,te.DeliveryModeId,te.Title,te.Cluster,te.ProgrammeAwardLevel,
                                 te.ProgrammeCertified,te.TargetAward,te.AwardAchievable,te.AwardSummary,te.ISCEDBroadUID,te.ISCEDDetailedFieldID,
                                 te.learnerfinishdate,te.learnerstartyear,te.finishmonth,te.finishyear,te.Gender,te.ParentFETProviderId,te.Parent,
                                 te.ParentDivisionProviderId,te.FETProviderTypeId,te.FETProviderName,te.IsETB,te.CountyId,
                                 te.programmecategorydescription,te.hasemployers,te.LearnerCountyId,te.NationalityId,te.outcomestatusdescription,
                                 te.outcomecertificationdescription,te.isoutcomecertified,te.outcomecertificationawarddescrip,
                                 te.outcomedescription,te.hash_ppsn,te.age                 
	                      from plss_tmp te
                          left join plss pl
                              on te.hash_ppsn = pl.hash_ppsn
                                  and te.CourseParticipationId = pl.CourseParticipationId
                                  and te.CourseCalendarId = pl.CourseCalendarId 
                                  and te.ApplicationId = pl.ApplicationId
                                  and te.DateStartCourse = pl.DateStartCourse
                                  and (te.DateActualFinishCourse = pl.DateActualFinishCourse or (te.DateActualFinishCourse is null and pl.DateActualFinishCourse is null))
                                  and (te.OutcomeId = pl.OutcomeId or (te.OutcomeId is null and pl.OutcomeId is null))
                                  and (te.DateDeleted = pl.DateDeleted or (te.DateDeleted is null and pl.DateDeleted is null))
                                  and (te.OutcomeStatusId = pl.OutcomeStatusId or (te.OutcomeStatusId is null and pl.OutcomeStatusId is null))
                                  and (te.OutcomeCertificationId = pl.OutcomeCertificationId or (te.OutcomeCertificationId is null and pl.OutcomeCertificationId is null))
                                  and (te.OutcomeCertificationAwardId = pl.OutcomeCertificationAwardId or (te.OutcomeCertificationAwardId is null and pl.OutcomeCertificationAwardId is null))
                                  and (te.OutcomeEarlyFinishReasonId = pl.OutcomeEarlyFinishReasonId or (te.OutcomeEarlyFinishReasonId is null and pl.OutcomeEarlyFinishReasonId is null))
                                  and (te.OriginalOutcomeId = pl.OriginalOutcomeId or (te.OriginalOutcomeId is null and pl.OriginalOutcomeId is null))
                                  and (te.ApplicationOriginId = pl.ApplicationOriginId or (te.ApplicationOriginId is null and pl.ApplicationOriginId is null))
                                  and (te.ApplicationStatusId = pl.ApplicationStatusId or (te.ApplicationStatusId is null and pl.ApplicationStatusId is null))
                                  and (te.FETProviderId = pl.FETProviderId or (te.FETProviderId is null and pl.FETProviderId is null))
                                  and (te.DeliveryFETProviderId = pl.DeliveryFETProviderId or (te.DeliveryFETProviderId is null and pl.DeliveryFETProviderId is null))
                                  and (te.ProgrammeId = pl.ProgrammeId or (te.ProgrammeId is null and pl.ProgrammeId is null))
                                  and (te.TargetAwardId = pl.TargetAwardId or (te.TargetAwardId is null and pl.TargetAwardId is null))
                                  and (te.PublishedCourseTitle = pl.PublishedCourseTitle or (te.PublishedCourseTitle is null and pl.PublishedCourseTitle is null))
                                  and (te.ProgrammeCategoryId = pl.ProgrammeCategoryId or (te.ProgrammeCategoryId is null and pl.ProgrammeCategoryId is null))
                                  and (te.DateActualStart = pl.DateActualStart or (te.DateActualStart is null and pl.DateActualStart is null))
                                  and (te.DateActualFinish = pl.DateActualFinish or (te.DateActualFinish is null and pl.DateActualFinish is null))
                                  and (te.DeliveryModeId = pl.DeliveryModeId or (te.DeliveryModeId is null and pl.DeliveryModeId is null))
                                  and te.Title = pl.Title 
                                  and (te.Cluster = pl.Cluster or (te.Cluster is null and pl.Cluster is null))
                                  and (te.ProgrammeAwardLevel = pl.ProgrammeAwardLevel or (te.ProgrammeAwardLevel is null and pl.ProgrammeAwardLevel is null))
                                  and (te.ProgrammeCertified = pl.ProgrammeCertified or (te.ProgrammeCertified is null and pl.ProgrammeCertified is null))
                                  and (te.TargetAward = pl.TargetAward or (te.TargetAward is null and pl.TargetAward is null))
                                  and (te.AwardAchievable = pl.AwardAchievable or (te.AwardAchievable is null and pl.AwardAchievable is null))
                                  and (te.AwardSummary = pl.AwardSummary or (te.AwardSummary is null and pl.AwardSummary is null))
                                  and (te.ISCEDBroadUID = pl.ISCEDBroadUID or (te.ISCEDBroadUID is null and pl.ISCEDBroadUID is null))
                                  and (te.ISCEDDetailedFieldID = pl.ISCEDDetailedFieldID or (te.ISCEDDetailedFieldID is null and pl.ISCEDDetailedFieldID is null))
                                  and (te.learnerfinishdate = pl.learnerfinishdate or (te.learnerfinishdate is null and pl.learnerfinishdate is null))
                                  and (te.learnerstartyear = pl.learnerstartyear or (te.learnerstartyear is null and pl.learnerstartyear is null))
                                  and (te.finishmonth = pl.finishmonth or (te.finishmonth is null and pl.finishmonth is null))
                                  and (te.finishyear = pl.finishyear or (te.finishyear is null and pl.finishyear is null))
                                  and (te.Gender = pl.Gender or (te.Gender is null and pl.Gender is null))
                                  and (te.ParentFETProviderId = pl.ParentFETProviderId or (te.ParentFETProviderId is null and pl.ParentFETProviderId is null))
                                  and (te.Parent = pl.Parent or (te.Parent is null and pl.Parent is null))
                                  and (te.ParentDivisionProviderId = pl.ParentDivisionProviderId or (te.ParentDivisionProviderId is null and pl.ParentDivisionProviderId is null))
                                  and (te.FETProviderTypeId = pl.FETProviderTypeId or (te.FETProviderTypeId is null and pl.FETProviderTypeId is null))
                                  and (te.FETProviderName = pl.FETProviderName or (te.FETProviderName is null and pl.FETProviderName is null))
                                  and (te.IsETB = pl.IsETB or (te.IsETB is null and pl.IsETB is null))
                                  and (te.CountyId = pl.CountyId or (te.CountyId is null and pl.CountyId is null))
                                  and (te.programmecategorydescription = pl.programmecategorydescription or (te.programmecategorydescription is null and pl.programmecategorydescription is null))
                                  and (te.hasemployers = pl.hasemployers or (te.hasemployers is null and pl.hasemployers is null))
                                  and (te.LearnerCountyId = pl.LearnerCountyId or (te.LearnerCountyId is null and pl.LearnerCountyId is null))
                                  and (te.NationalityId = pl.NationalityId or (te.NationalityId is null and pl.NationalityId is null))
                                  and (te.outcomestatusdescription = pl.outcomestatusdescription or (te.outcomestatusdescription is null and pl.outcomestatusdescription is null))
                                  and (te.outcomecertificationdescription = pl.outcomecertificationdescription or (te.outcomecertificationdescription is null and pl.outcomecertificationdescription is null))
                                  and (te.isoutcomecertified = pl.isoutcomecertified or (te.isoutcomecertified is null and pl.isoutcomecertified is null))
                                  and (te.outcomecertificationawarddescrip = pl.outcomecertificationawarddescrip or (te.outcomecertificationawarddescrip is null and pl.outcomecertificationawarddescrip is null))
                                  and (te.outcomedescription = pl.outcomedescription or (te.outcomedescription is null and pl.outcomedescription is null))
                                  and (te.age = pl.age or (te.age is null and pl.age is null))
                          where pl.hash_ppsn is null
                 """)
        conn.execute(t)
        print('------  add >' + str(datetime.datetime.now()))
        t = text("""
                      delete
                          from plss
                          where id in (select pl.id
                                           from plss pl
                                               left join plss_tmp te
                                                   on te.hash_ppsn = pl.hash_ppsn
                                                       and (te.CourseParticipationId = pl.CourseParticipationId or (te.CourseParticipationId is null and pl.CourseParticipationId is null))
                                                       and (te.CourseCalendarId = pl.CourseCalendarId or (te.CourseCalendarId is null and pl.CourseCalendarId is null))
                                                       and (te.ApplicationId = pl.ApplicationId or (te.ApplicationId is null and pl.ApplicationId is null))
                                                       and (te.DateStartCourse = pl.DateStartCourse or (te.DateStartCourse is null and pl.DateStartCourse is null))
                                                       and (te.DateActualFinishCourse = pl.DateActualFinishCourse or (te.DateActualFinishCourse is null and pl.DateActualFinishCourse is null))
                                                       and (te.OutcomeId = pl.OutcomeId or (te.OutcomeId is null and pl.OutcomeId is null))
                                                       and (te.DateDeleted = pl.DateDeleted or (te.DateDeleted is null and pl.DateDeleted is null))
                                                       and (te.OutcomeStatusId = pl.OutcomeStatusId or (te.OutcomeStatusId is null and pl.OutcomeStatusId is null))
                                                       and (te.OutcomeCertificationId = pl.OutcomeCertificationId or (te.OutcomeCertificationId is null and pl.OutcomeCertificationId is null))
                                                       and (te.OutcomeCertificationAwardId = pl.OutcomeCertificationAwardId or (te.OutcomeCertificationAwardId is null and pl.OutcomeCertificationAwardId is null))
                                                       and (te.OutcomeEarlyFinishReasonId = pl.OutcomeEarlyFinishReasonId or (te.OutcomeEarlyFinishReasonId is null and pl.OutcomeEarlyFinishReasonId is null))
                                                       and (te.OriginalOutcomeId = pl.OriginalOutcomeId or (te.OriginalOutcomeId is null and pl.OriginalOutcomeId is null))
                                                       and (te.ApplicationOriginId = pl.ApplicationOriginId or (te.ApplicationOriginId is null and pl.ApplicationOriginId is null))
                                                       and (te.ApplicationStatusId = pl.ApplicationStatusId or (te.ApplicationStatusId is null and pl.ApplicationStatusId is null))
                                                       and (te.FETProviderId = pl.FETProviderId or (te.FETProviderId is null and pl.FETProviderId is null))
                                                       and (te.DeliveryFETProviderId = pl.DeliveryFETProviderId or (te.DeliveryFETProviderId is null and pl.DeliveryFETProviderId is null))
                                                       and (te.ProgrammeId = pl.ProgrammeId or (te.ProgrammeId is null and pl.ProgrammeId is null))
                                                       and (te.TargetAwardId = pl.TargetAwardId or (te.TargetAwardId is null and pl.TargetAwardId is null))
                                                       and (te.PublishedCourseTitle = pl.PublishedCourseTitle or (te.PublishedCourseTitle is null and pl.PublishedCourseTitle is null))
                                                       and (te.ProgrammeCategoryId = pl.ProgrammeCategoryId or (te.ProgrammeCategoryId is null and pl.ProgrammeCategoryId is null))
                                                       and (te.DateActualStart = pl.DateActualStart or (te.DateActualStart is null and pl.DateActualStart is null))
                                                       and (te.DateActualFinish = pl.DateActualFinish or (te.DateActualFinish is null and pl.DateActualFinish is null))
                                                       and (te.DeliveryModeId = pl.DeliveryModeId or (te.DeliveryModeId is null and pl.DeliveryModeId is null))
                                                       and (te.Title = pl.Title or (te.Title is null and pl.Title is null))
                                                       and (te.Cluster = pl.Cluster or (te.Cluster is null and pl.Cluster is null))
                                                       and (te.ProgrammeAwardLevel = pl.ProgrammeAwardLevel or (te.ProgrammeAwardLevel is null and pl.ProgrammeAwardLevel is null))
                                                       and (te.ProgrammeCertified = pl.ProgrammeCertified or (te.ProgrammeCertified is null and pl.ProgrammeCertified is null))
                                                       and (te.TargetAward = pl.TargetAward or (te.TargetAward is null and pl.TargetAward is null))
                                                       and (te.AwardAchievable = pl.AwardAchievable or (te.AwardAchievable is null and pl.AwardAchievable is null))
                                                       and (te.AwardSummary = pl.AwardSummary or (te.AwardSummary is null and pl.AwardSummary is null))
                                                       and (te.ISCEDBroadUID = pl.ISCEDBroadUID or (te.ISCEDBroadUID is null and pl.ISCEDBroadUID is null))
                                                       and (te.ISCEDDetailedFieldID = pl.ISCEDDetailedFieldID or (te.ISCEDDetailedFieldID is null and pl.ISCEDDetailedFieldID is null))
                                                       and (te.learnerfinishdate = pl.learnerfinishdate or (te.learnerfinishdate is null and pl.learnerfinishdate is null))
                                                       and (te.learnerstartyear = pl.learnerstartyear or (te.learnerstartyear is null and pl.learnerstartyear is null))
                                                       and (te.finishmonth = pl.finishmonth or (te.finishmonth is null and pl.finishmonth is null))
                                                       and (te.finishyear = pl.finishyear or (te.finishyear is null and pl.finishyear is null))
                                                       and (te.Gender = pl.Gender or (te.Gender is null and pl.Gender is null))
                                                       and (te.ParentFETProviderId = pl.ParentFETProviderId or (te.ParentFETProviderId is null and pl.ParentFETProviderId is null))
                                                       and (te.Parent = pl.Parent or (te.Parent is null and pl.Parent is null))
                                                       and (te.ParentDivisionProviderId = pl.ParentDivisionProviderId or (te.ParentDivisionProviderId is null and pl.ParentDivisionProviderId is null))
                                                       and (te.FETProviderTypeId = pl.FETProviderTypeId or (te.FETProviderTypeId is null and pl.FETProviderTypeId is null))
                                                       and (te.FETProviderName = pl.FETProviderName or (te.FETProviderName is null and pl.FETProviderName is null))
                                                       and (te.IsETB = pl.IsETB or (te.IsETB is null and pl.IsETB is null))
                                                       and (te.CountyId = pl.CountyId or (te.CountyId is null and pl.CountyId is null))
                                                       and (te.programmecategorydescription = pl.programmecategorydescription or (te.programmecategorydescription is null and pl.programmecategorydescription is null))
                                                       and (te.hasemployers = pl.hasemployers or (te.hasemployers is null and pl.hasemployers is null))
                                                       and (te.LearnerCountyId = pl.LearnerCountyId or (te.LearnerCountyId is null and pl.LearnerCountyId is null))
                                                       and (te.NationalityId = pl.NationalityId or (te.NationalityId is null and pl.NationalityId is null))
                                                       and (te.outcomestatusdescription = pl.outcomestatusdescription or (te.outcomestatusdescription is null and pl.outcomestatusdescription is null))
                                                       and (te.outcomecertificationdescription = pl.outcomecertificationdescription or (te.outcomecertificationdescription is null and pl.outcomecertificationdescription is null))
                                                       and (te.isoutcomecertified = pl.isoutcomecertified or (te.isoutcomecertified is null and pl.isoutcomecertified is null))
                                                       and (te.outcomecertificationawarddescrip = pl.outcomecertificationawarddescrip or (te.outcomecertificationawarddescrip is null and pl.outcomecertificationawarddescrip is null))
                                                       and (te.outcomedescription = pl.outcomedescription or (te.outcomedescription is null and pl.outcomedescription is null))
                                                       and (te.age = pl.age or (te.age is null and pl.age is null))
                                               where pl.hash_ppsn is null)
                   """)
        conn.execute(t)
        print('------  del >' + str(datetime.datetime.now()))
        t = text("drop table plss_tmp")
        conn.execute(t)
        print('-----  drop >' + str(datetime.datetime.now()))

