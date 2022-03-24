#include "abmprompt.h"
#include "ADataSet.h"  //使用里面的trim函数
#include "strconv.h"

int32 ABMPrompt::get_cfg(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    m_pCfg = NAMESPACE_OD_APP_CFG AppReadConfig<CPromptCfg>();

    if (m_pCfg == NULL)
    {
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> func: %s, init configuration failed,please check!", __func__);
        return ABMPROMPT_ERROR;
    }
    return ABMPROMPT_OK;
}

//整合业务开始前系统参数读取
int32 ABMPrompt::get_ParamXc(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    //1、捞取省份特殊业务分支
    m_iSystemBaseType = ENUM_SYSTEM_TYPE_HUNAN; //默认湖南分支
    aistring strTmp;
    get_sysParamter("ENUM_SYSTEM_BASE_TYPE", strTmp);
    m_iSystemBaseType = atoi(strTmp.c_str());
    m_crmCaller.set_provinceFlag(m_iSystemBaseType);
    LOG_TRACE("ENUM_SYSTEM_BASE_TYPE:%d ... ", m_iSystemBaseType);

    return ABMPROMPT_OK;
}

time_t ABMPrompt::get_xcValidTime()
{
    xc::CSnapshot cSnap("ZW::ABM_BALANCE");
    return cSnap.GetValidTime();
}

int32 ABMPrompt::program_run(SOBSession *pSession, int32 &iRet, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    if (get_cfg(pSession, cErrorMsg) == -1)
    {
        return ABMPROMPT_ERROR;
    }

    m_isNeedUpdateStatus = false; //第一次需要更新状态(更新小于-1的状态)
    //调用crm接口如果出现网络原因失败，更新原则:
    // 1、程序启动直接更新 status<-1的工单
    // 2、如果扫描数据为空，根据条件判断是否需要更新status< -1的工单
    // 3、如果出现网络连接不通的工单：m_isNeedUpdateStatus = true;
    // 4、判断是否有网络不通的错误工单(判断m_isNeedUpdateStatus=true)，
    //    如果有，且本次工单网络调通，则更新status<-1的工单，且赋值m_isNeedUpdateStatus = false;
    bool isFirst = true;
    m_strAppType = m_pCfg->m_cfgParams.m_strType;
    //get_partitionTbl(pSession, m_pCfg->m_cfgParams.m_nTablePartNo,m_strAppType, m_strDBName, m_sourceTable,cErrorMsg);
    m_iUpdateCrmExcep = m_pCfg->m_cfgCommon.m_updateCrmExcep;
    m_iUpdateCrmExcep = m_iUpdateCrmExcep >= 0 ? m_iUpdateCrmExcep : PROMPT_UPDATE_CRM_EXCEPTION;
    aistring retrycode(m_pCfg->m_cfgCommon.m_needReTryCode);
    CStringList codeList;
    cdk::strings::Split(retrycode, ":", codeList);
    for (CStringList::iterator it = codeList.begin(); it != codeList.end(); ++it)
    {
        m_retryCode.insert(*it);
    }
    //BOMC日志输出设置，目前只对开机生成
    if ((ABM_PROMPT_APP_TYPE_STS == m_strAppType) && (m_pCfg->m_cfgParams.m_stsType == STS_TYPE_OPEN))
    {
        //设置运行参数
        m_bomcFile.setProcessParam(m_pCfg->m_cfgParams.m_nPNo, m_pCfg->m_cfgParams.m_nTablePartNo, COSTTIME_FRONTTOCUR);
        //设置BOMC文件路径
        m_bomcFile.setOutPath(m_pCfg->m_cfgParams.m_bomcLogPath);
    }

    m_iCallFailCount = 0; //记录调用夹带短信接口连续失败的次数

    while (NAMESPACE_SERV_RUN_STAT RUNNING == NAMESPACE_SERV_RUN_STAT GetState())
    {
        time_t tmValidTime = get_xcValidTime();
        if (m_tmXcRefresh != tmValidTime)
        {
            LOG_TRACE("XC Refreshed, need to get params from XC again ... ");
            get_ParamXc(pSession, cErrorMsg);
            m_tmXcRefresh = tmValidTime;
            LOG_TRACE("xc last refresh time[m_tmXcRefresh] = %ld ... ", m_tmXcRefresh);
        }

        m_dtDateTime = CBSDateTime::currentDateTime();
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->program_run")

        //陕西的工单表表名里添加了地市编码
        AISTD string strRegionCode = AISTD string(m_pCfg->m_cfgParams.m_szRegionCode);
        //云南添加获取基表 yunnan_v8 add by sunph3 20210914
        int16 nBaseTable = m_pCfg->m_cfgParams.m_nBaseTable;

        m_isNeedGroup = true;  // 默认是需要入明细表 add by ligc@20200720

        if (ABM_PROMPT_APP_TYPE_CREDIT == m_strAppType)
        {
            m_nActionLevel = 0; //飞书必达平台工单标识
            if(ENUM_SYSTEM_TYPE_QINGHAI == m_iSystemBaseType)
            {
            	m_hisTable = "IMS_NTF_CREDIT_HIS_" + strRegionCode + "_";
            	m_sourceTable = "IMS_NTF_CREDIT_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else if(ENUM_SYSTEM_TYPE_HUNAN == m_iSystemBaseType)
            {
            	m_hisTable = "IMS_NTF_CREDIT_HIS_";
                if(nBaseTable == 0) {
                    m_sourceTable = "IMS_NTF_CREDIT_" + strRegionCode + "_" +cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
                } else if (nBaseTable == 1) {
                    m_sourceTable = "IMS_NTF_CREDIT";
                }
            }
            else
            {
            	LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> SYSTEM TYPE[%ld] ERROR,PLEASE CHECK!!!!",m_iSystemBaseType);
            	return SDL_FAIL;
            }
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            aistring strtmp;
            int32 iRet = get_sysParamter("CALL_SMS_SERVICE_FLAG", strtmp);
            if (ABMPROMPT_OK != iRet)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> get CALL_SMS_SERVICE_FLAG fail, use default 0");
                m_isCallSmsService = false;
            }
            else
            {
                if ("1" == strtmp)
                {
                    m_isCallSmsService = true;
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> is need call sms service!!!");
                }
                else
                {
                    m_isCallSmsService = false;
                }
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit")
            process_credit(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if( ABM_PROMPT_APP_TYPE_GRPCREDIT == m_strAppType)
        {
            m_nActionLevel = 0;//飞书必达平台工单标识
            m_hisTable = "IMS_NTF_GRPCREDIT_HIS_";
            m_sourceTable = "IMS_NTF_GRPCREDIT_"+cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            // add by ligc@20190429 短信夹带
            aistring strtmp;
            int32 iRet = get_sysParamter("CALL_SMS_SERVICE_FLAG", strtmp);
            if(ABMPROMPT_OK != iRet)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> get CALL_SMS_SERVICE_FLAG fail, use default 0");
                m_isCallSmsService = false;
            }
            else
            {
                if("1" == strtmp)
                {
                    m_isCallSmsService = true;
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> is need call sms service!!!");
                }
                else
                {
                    m_isCallSmsService = false;
                }
            }
            // end add
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit")
            process_grpcredit(pSession,cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_STS == m_strAppType)
        {
        	if(ENUM_SYSTEM_TYPE_QINGHAI == m_iSystemBaseType)
        	{
           		m_hisErrTable = "IMS_NTF_STSERR_HIS_"+strRegionCode+"_"; //qiankun3 20190304 信控错单优化
            	m_hisTable = "IMS_NTF_STS_HIS_" + strRegionCode + "_";
            	m_sourceTable = "IMS_NTF_STS_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else if (ENUM_SYSTEM_TYPE_HUNAN == m_iSystemBaseType)
            {
            	m_hisErrTable = "IMS_NTF_STSERR_HIS_"; //qiankun3 20190304 信控错单优化
            	m_hisTable = "IMS_NTF_STS_HIS_";
                if(nBaseTable == 0) {
            	    m_sourceTable = "IMS_NTF_STS_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
                } else if (nBaseTable == 1) {
                    m_sourceTable = "IMS_NTF_STS"; 
                }
            }
            else
            {
            	LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> SYSTEM TYPE[%ld] ERROR,PLEASE CHECK!!!!",m_iSystemBaseType);
            	return SDL_FAIL;
            }
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            //程序启动直接更新有状态status<-1的记录
            if (isFirst)
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                update_status<MAbmInterfacePromptDb::CImsNtfStsList, MAbmInterfacePromptDb::CImsNtfSts>(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts")
            //取休眠工单数的阀值 by xuxl3@asiainfo.com@20170215
            AISTD string strTmp = "";
            thresholdForSleep = 0;
            int32 iRet = get_sysParamter("NOTIFICATION_SLEEP_WORKS", strTmp);
            if (iRet != ABMPROMPT_OK)
            {
                LOG_TRACE("\n==[ABM_NOTI_FILTER]==> can not find = NOTIFICATION_SLEEP_WORKS from SYS_PARAMTER use default 0 ");
            }
            else
            {
                thresholdForSleep = atoi(strTmp.c_str());
            }
            process_sts(pSession, cErrorMsg);
            ES_END_RUN_TIME
            isFirst = false;
        }
        else if( ABM_PROMPT_APP_TYPE_GRPSTS == m_strAppType)
        {
            m_hisErrTable = "IMS_NTF_GRPSTSERR_HIS_";//qiankun3 20190304 信控错单优化 政企停开机，催缴暂定10张分表
            m_hisTable = "IMS_NTF_GRPSTS_HIS_";
            m_sourceTable = "IMS_NTF_GRPSTS_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            //程序启动直接更新有状态status<-1的记录
            if (isFirst)
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                update_status<MAbmInterfacePromptDb::CImsNtfGrpstsList, MAbmInterfacePromptDb::CImsNtfGrpsts>(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts")
            process_grpsts(pSession,cErrorMsg);
            ES_END_RUN_TIME
            isFirst = false;
        }
        else if (ABM_PROMPT_APP_TYPE_REMIND == m_strAppType)
        {
            m_nActionLevel = 0; //飞书必达平台工单标识
            if(ENUM_SYSTEM_TYPE_QINGHAI == m_iSystemBaseType)
            {
	            m_hisTable = "IMS_NTF_REMIND_HIS_" + strRegionCode + "_";
	            m_sourceTable = "IMS_NTF_REMIND_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else if (ENUM_SYSTEM_TYPE_HUNAN == m_iSystemBaseType)
            {
            	m_hisTable = "IMS_NTF_REMIND_HIS_";
	            // mod by ligc@20190812 for HNM_REQ_20190809_0002 流量提醒能力提升的优化需求
	            // 湖南remind表进行扩表，按地州+账户
	            m_sourceTable = "IMS_NTF_REMIND_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else
            {
            	LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> SYSTEM TYPE[%ld] ERROR,PLEASE CHECK!!!!",m_iSystemBaseType);
            	return SDL_FAIL;
            }
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            // 获取达量优速速率接口名称
            int32 iRet = get_sysParamter("ABM_QUERY_LIMIT_SPEED_NAME", m_crmHttpQueryLimitSpeedName);
            if (iRet != ABMPROMPT_OK)
            {
                LOG_TRACE("\n==[ABM_NOTI_FILTER]==> can not find = ABM_QUERY_LIMIT_SPEED_NAME from SYS_PARAMTER use default SS.PccActionSVC.queryLimitSpeed ");
                m_crmHttpQueryLimitSpeedName = "SS.PccActionSVC.queryLimitSpeed";
            }

            aistring strtmp;
            iRet = get_sysParamter("CALL_SMS_SERVICE_FLAG", strtmp);
            if (ABMPROMPT_OK != iRet)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> get CALL_SMS_SERVICE_FLAG fail, use default 0");
                m_isCallSmsService = false;
            }
            else
            {
                if ("1" == strtmp)
                {
                    m_isCallSmsService = true;
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> is need call sms service!!!");
                }
                else
                {
                    m_isCallSmsService = false;
                }
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind")
            process_remind(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_REMINDONTIME == m_strAppType)
        {
            m_nActionLevel = 0; //飞书必达平台工单标识
            if(ENUM_SYSTEM_TYPE_QINGHAI == m_iSystemBaseType)
            {
	            m_hisTable = "IMS_NTF_ONTIME_HIS_" + strRegionCode + "_";
	            m_sourceTable = "IMS_NTF_ONTIME_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else if (ENUM_SYSTEM_TYPE_HUNAN == m_iSystemBaseType)
            {
	            m_hisTable = "IMS_NTF_ONTIME_HIS_";
	            m_sourceTable = "IMS_NTF_ONTIME_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else
            {
            	LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> SYSTEM TYPE[%ld] ERROR,PLEASE CHECK!!!!",m_iSystemBaseType);
            	return SDL_FAIL;
            }
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;

            // 获取达量优速速率接口名称
            int32 iRet = get_sysParamter("ABM_QUERY_LIMIT_SPEED_NAME", m_crmHttpQueryLimitSpeedName);
            if (iRet != ABMPROMPT_OK)
            {
                LOG_TRACE("\n==[ABM_NOTI_FILTER]==> can not find = ABM_QUERY_LIMIT_SPEED_NAME from SYS_PARAMTER use default SS.PccActionSVC.queryLimitSpeed ");
                m_crmHttpQueryLimitSpeedName = "SS.PccActionSVC.queryLimitSpeed";
            }

            aistring strtmp;
            iRet = get_sysParamter("CALL_SMS_SERVICE_FLAG", strtmp);
            if (ABMPROMPT_OK != iRet)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> get CALL_SMS_SERVICE_FLAG fail, use default 0");
                m_isCallSmsService = false;
            }
            else
            {
                if ("1" == strtmp)
                {
                    m_isCallSmsService = true;
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> is need call sms service!!!");
                }
                else
                {
                    m_isCallSmsService = false;
                }
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remindontime");
            process_remind(pSession, cErrorMsg); //陕西移动定期提醒的处理流程和实时提醒的一样
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_SERVICE == m_strAppType)
        {
        	if(ENUM_SYSTEM_TYPE_QINGHAI == m_iSystemBaseType)
        	{
	            m_hisTable = "IMS_NTF_SERV_HIS_" + strRegionCode + "_";
	            m_hisRateTable = "IMS_NTF_SERV_RATE_" + strRegionCode + "_";
	            m_sourceTable = "IMS_NTF_SERV_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else if (ENUM_SYSTEM_TYPE_HUNAN == m_iSystemBaseType)
            {
            	m_hisTable = "IMS_NTF_SERVICE_HIS_";
	            m_hisRateTable = "IMS_NTF_SERVICE_RATE_"; //移动商城2.8 qiankun3
				m_sourceTable = "IMS_NTF_SERVICE_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
	            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            }
            else
            {
            	LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> SYSTEM TYPE[%ld] ERROR,PLEASE CHECK!!!!",m_iSystemBaseType);
            	return SDL_FAIL;
            }
            if (isFirst)
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                update_status<MAbmInterfacePromptDb::CImsNtfServList, MAbmInterfacePromptDb::CImsNtfServ>(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_service")
            process_service(pSession, cErrorMsg);
            ES_END_RUN_TIME
            isFirst = false;
        }
        else if (ABM_PROMPT_APP_TYPE_TOREAD == m_strAppType)
        {
            m_hisTable = "IMS_NTF_TOREAD_HIS_";
            m_sourceTable = "IMS_NTF_TOREAD_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            ;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;

            if (isFirst)
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                update_status<MAbmInterfacePromptDb::CImsNtfToreadList, MAbmInterfacePromptDb::CImsNtfToread>(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_toread")
            process_toread(pSession, cErrorMsg);
            ES_END_RUN_TIME
            isFirst = false;
        }
        else if (ABM_PROMPT_APP_TYPE_CONFIRM == m_strAppType)
        {
        	if(ENUM_SYSTEM_TYPE_QINGHAI == m_iSystemBaseType)
        	{
            	m_hisTable = "IMS_NTF_CONF_HIS_" + strRegionCode + "_";
            	m_sourceTable = "IMS_NTF_CONF_" + strRegionCode + "_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            }
            else if(ENUM_SYSTEM_TYPE_HUNAN == m_iSystemBaseType)
            {
            	m_hisTable = "IMS_NTF_CONFIRM_HIS_";
				m_sourceTable = "IMS_NTF_CONFIRM_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);;
            	m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            }
            else
            {
            	LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> SYSTEM TYPE[%ld] ERROR,PLEASE CHECK!!!!",m_iSystemBaseType);
            	return SDL_FAIL;
            }
            if (isFirst)
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                update_status<MAbmInterfacePromptDb::CImsNtfConfList, MAbmInterfacePromptDb::CImsNtfConf>(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm")
            process_confirm(pSession, cErrorMsg);
            ES_END_RUN_TIME
            isFirst = false;
        }
        else if (ABM_PROMPT_APP_TYPE_WECHAT == m_strAppType)
        {
            m_nActionLevel = 0; //飞书必达平台工单标识
            m_hisTable = "IMS_NTF_WECHAT_HIS_";
            m_sourceTable = "IMS_NTF_WECHAT_" + cdk::strings::Itoa(m_pCfg->m_cfgParams.m_nTablePartNo);
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_wechat")
            process_wechat(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_MERGECREDIT == m_strAppType) // add by ligc@20181031 for 湖南政企催缴工单合并
        {
            m_hisTable = "IMS_NTF_MERGECREDIT_HIS_";
            m_sourceTable = "IMS_NTF_MERGECREDIT";
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_iMergeNum = m_pCfg->m_cfgParams.m_nMergeNum;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergecredit")
            process_mergecredit(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_MERGEWARN == m_strAppType) // add by ligc@20181031 for 湖南政企预警工单合并
        {
            m_hisTable = "IMS_NTF_MERGEWARN_HIS_";
            m_sourceTable = "IMS_NTF_MERGEWARN";
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_iMergeNum = m_pCfg->m_cfgParams.m_nMergeNum;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergewarn")
            process_mergewarn(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_MERGESTOP == m_strAppType) // add by ligc@20190618 for 湖南政企停机短信工单合并
        {
            m_hisTable = "IMS_NTF_MERGESTOP_HIS_";
            m_sourceTable = "IMS_NTF_MERGESTOP";
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_iMergeNum = m_pCfg->m_cfgParams.m_nMergeNum;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergestop")
            process_mergestop(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_IVR == m_strAppType) // add by taocj@2021 超套优化
        {
            m_hisTable = "IMS_NTF_IVR_HIS_";
            m_sourceTable = "IMS_NTF_IVR";
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_ivr")
            process_ivr(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        // add by xupp3 for yunnanV8 Begin
        else if (ABM_PROMPT_APP_TYPE_MERGESMS == m_strAppType) 
        {
            m_hisTable = "IMS_NTF_MERGECREDIT_HIS_";
            m_sourceTable = "IMS_NTF_MERGECREDIT";
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_iMergeNum = m_pCfg->m_cfgParams.m_nMergeNum;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergestop")
            process_mergesms(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_MERGESMSWARN == m_strAppType) 
        {
            m_hisTable = "IMS_NTF_MERGEWARN_HIS_";
            m_sourceTable = "IMS_NTF_MERGEWARN";
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_iMergeNum = m_pCfg->m_cfgParams.m_nMergeNum;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergestop")
            process_mergesmsWarn(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        else if (ABM_PROMPT_APP_TYPE_MERGESMSSTOP == m_strAppType) 
        {
            m_hisTable = "IMS_NTF_MERGESTOP_HIS_";
            m_sourceTable = "IMS_NTF_MERGESTOP";
            m_hisGroupTable = "IMS_NTF_GROUP_HIS_";
            m_iMergeNum = m_pCfg->m_cfgParams.m_nMergeNum;
            m_smsTable = m_pCfg->m_cfgCommon.m_smsTabName;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergestop")
            process_mergesmsStop(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        // add by xupp3 for yunnanV8 End
        else
        {
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> func: %s, app_type = %s undefined ,please check confiugre file!", __func__, m_strAppType.c_str());
            break;
        }
        ES_END_RUN_TIME

        ABM_PROMPT_STAT

        //休眠配置中的秒数, -1表示执行一次，大于等于0表示循环执行
        if (m_pCfg->m_cfgCommon.m_nSleep < 0)
        {
            return SDL_ONCE; // -1 直接退出
        }
    }
    LEAVE_FUNC;

    ABM_PROMPT_STAT
    return ABMPROMPT_OK;
}

int32 ABMPrompt::call_crmService(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    SYSTEM_BASE_TYPE_HUNAN_IF
    return call_crmService_HN(pSession, cErrorMsg);
    SYSTEM_BASE_TYPE_QINHAI_ELIF
    return call_crmService_QH(pSession, cErrorMsg);
    SYSTEM_BASE_TYPE_END
    else
    {
        LOG_ERROR(ABMPROMPT_XC_ERROR, "====[ABM_SERV_ABMPROMPT]===> call_crmService FAILED,m_iSystemBaseType:%d...", m_iSystemBaseType);
    }
}

int32 ABMPrompt::call_crmService_HN(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    if (get_svcURI(pSession, cErrorMsg) != ABMPROMPT_OK)
    {
        LOG_ERROR(ABMPROMPT_XC_ERROR, "====[ABM_SERV_ABMPROMPT]===> get_svcURI failed");
        return ABMPROMPT_ERROR;
    }

    //当模拟调crm时，用耗时usleep来模拟调用时间
    if (PROMPT_NO_CALL_CRM == m_pCfg->m_cfgCommon.m_nNoCallCrm)
    {
        // usleep 参数单位为us，1s = 10^6 us
        usleep(static_cast<uint32>(m_pCfg->m_cfgCommon.m_dSimSleep * 1000000));
        cErrorMsg.set_hint("sleep simulate call CRM!");
        cErrorMsg.set_errorMsg("NO call CRM");
        LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===>sleep is used to simulate calling crm , elapsed %f s", m_pCfg->m_cfgCommon.m_dSimSleep);
        return ABMPROMPT_OK;
    }

    //qiankun3 20190705 crm安全日志 获取本机ID
    string sLoginIp = "";
    string sRemoteAddr = "";
    string sClientIp = "";
    if (ABMPROMPT_OK == getLocalIp(sLoginIp))
    {
        sRemoteAddr = sLoginIp;
        sClientIp = sLoginIp;
    }
    LOG_TRACE("LgoinIp:%s", sLoginIp.c_str());

    if (ABM_PROMPT_APP_TYPE_STS == m_strAppType)
    {
        //停开机传的用户地州
        AISTD string userRgCode = m_imsNtfSts.get_extend3();
        if (userRgCode.empty())
        {
            char reginStr[10];
            snprintf(reginStr, sizeof(reginStr), "%04d", m_imsNtfSts.get_regionCode());
            userRgCode = string(reginStr);
        }
        //char reginStr[10];
        //snprintf(reginStr, sizeof(reginStr), "0%d", m_imsNtfSts.get_regionCode());
        AISTD string retInfo;
        AISTD string staff_id("CREDIT00");
        AISTD string depart_id("CREDI");
        if (m_imsNtfSts.get_notificationType() == AREA_STOP_NOTIFY_TYPE)
        {
            staff_id = "area";
            depart_id = "boss";
        }

        //手工信控停机员工ID
        AISTD string strStaffId = m_imsNtfSts.get_staffId();
        LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> STAFF ID:%s", strStaffId.c_str());

        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendStsToCrm(m_svcURI,
                                                          m_crmHttpServiceName,
                                                          "",
                                                          "",
                                                          userRgCode,
                                                          m_imsNtfSts.get_extend3(),
                                                          staff_id,  // "CREDIT00",
                                                          depart_id, //"CREDI",
                                                          m_imsNtfSts.get_resourceId(),
                                                          m_imsNtfSts.get_phoneId(),
                                                          m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(),
                                                          CALL_CRM_SEQUECE_BASE + m_imsNtfSts.get_queueId(), //sequece_id
                                                          "00",                                              //网别统一填00,没有铁通
                                                          m_imsNtfSts.get_remark(),
                                                          sRemoteAddr,
                                                          sLoginIp,
                                                          sClientIp,
                                                          strStaffId,
                                                          retInfo);
        CALL_CRM_RETRY_END

        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);
        //751013:业务受理前条件判断-用户有未完工的限制业务【缴费复机】！\n214325●办理[7302]业务，用户有未完工的该业务，不需要再处理！\n
        //CRM_COMM_982:用户有未完工的订单[集团信控恢复][3296102897270047][2016-10-28 00:52:28]请稍后！
        //CRM_USER_1013:用户不是欠费预销号状态，不能进行缴费复机操作！
        //115003:办理[7301]业务，用户原服务状态不满足[不能生成服务状态台帐]！\n
        //CRM_TRADE_9:台帐表中已存在对该用户的注销工单，信控已无含义！
        //CRM_GRP_96:该集团已经是正常状态！
        //CRM_USER_15:CRM中用户已处于正常状态，不需要再恢复！
        //CRM_USER_1013:用户不是欠费预销号状态，不能进行缴费复机操作！

        //CRM_TRADE_88:该用户台账表中有其他工单 暂时不能受理！
        //CRM_BOF_014:根据当前TRADE_TYPE_CODE找不到符合条件的BuilderRequestData
        //115999:没有获取到业务类型[null]对应的服务状态变更参数！用户编号[3114070340105357]\n
        //CRM_BIZ_654:该服务号码[13789214370]路由信息不存在
        //CRM_BOF_001:无法获取TradeType数据   TRADE_TYPE_CODE=%
        //CRM_TRADE_325:获取业务类型无数据，业务类型[7301]
        if (m_strCallCrmResultCode == "0")
        {
            return ABMPROMPT_OK;
        }
        else if (m_retryCode.find(m_strCallCrmResultCode) != m_retryCode.end())
        {
            return ABMPROMPT_SLOWSPEED;
            //CODE_CALLCRM_SLOWSPEED        = EPNCODE_TRADE_RESULTCODE + 1; //CRM侧返回需要降速，CRM错误码：CRM_TRADECREDIT_999
            //return 2;
        }
        else if (m_strCallCrmResultCode == "CRM_TRADE_115040" ||
                m_strCallCrmResultCode == "CRM_TRADE_115041" ||
                m_strCallCrmResultCode == "CRM_TRADE_115042" ||
                 m_strCallCrmResultCode == "115003" ||
                 m_strCallCrmResultCode == "214325" ||
                 m_strCallCrmResultCode == "214327" ||
                 m_strCallCrmResultCode == "CRM_TRADE_912030" ||
                 m_strCallCrmResultCode == "751013" ||
                 m_strCallCrmResultCode == "CRM_COMM_982" ||
                 m_strCallCrmResultCode == "CRM_TRADE_7")
        {
            return ABMPROMPT_REPEATED;
            //办理[7110]业务，用户状态已经是该状态或存在未完工的
            //变更到该状态的工单，不需要再开通！用户编号[3110110325670010]服务编码[0]状态[7]
            if (m_strCallCrmResultCode == "CRM_TRADE_115040" ||m_strCallCrmResultCode == "CRM_TRADE_115041"||m_strCallCrmResultCode == "CRM_TRADE_115042"|| m_strCallCrmResultCode == "CRM_TRADE_7")
            {
                //CODE_CALLCRM_SAMETRADE        = EPNCODE_TRADE_RESULTCODE + 3; //CRM侧台帐表中已存在该业务工单，多次发重复工单，CRM侧只接收第一笔，后续工单拒绝接收，CRM错误码：214325
            }
            else if (m_strCallCrmResultCode == "214325")
            {
                //CODE_CALLCRM_HASSTATECHANGE   = EPNCODE_TRADE_RESULTCODE + 2; //CRM侧用户已处于该业务类型状态，不需要再开通，CRM错误码：CRM_TRADE_115040
            }
            else if (m_strCallCrmResultCode == "CRM_TRADE_912030")
            {
                //CODE_CALLCRM_NOTCTRLPROD      = EPNCODE_TRADE_RESULTCODE + 4; //CRM不信控产品
            }
            else
            {
                //CODE_CALLCRM_DIRECTFINISH     = EPNCODE_TRADE_RESULTCODE + 5; //CRM不需要处理
            }
            //log direct finish
        }
        else
        {

            if (m_strCallCrmResultCode == "-100") //CRM返回字符串解析错误
            {
                //CODE_CALLCRM_DESERIALIZE      = EPNCODE_TRADE_RESULTCODE + 31; //CRM返回字符串解析错误
            }
            else if (m_strCallCrmResultCode == "-101" || m_strCallCrmResultCode == "-102")
            {

                if (m_strCallCrmResultCode == "-101") //调用服务失败，自动延迟调用三次，超过三次，再置错误
                {
                    /* 调crm接口自动重复调用
                    int t_recallCount = atoi(m_pTrade->m_manageTag.substr(9,1).c_str());
                    if (t_recallCount < 3)  //相隔5分钟，最多重复执行3次
                    {
                        char t_tagBuff[10];
                        sprintf(t_tagBuff,"%s%d",m_pTrade->m_manageTag.substr(0,9).c_str(),t_recallCount+1);
                        m_pCdr->m_manageTag = t_tagBuff;
                        m_pCdr->m_delayInterval = 5; //延迟5分钟
                        return 0;
                    }
                    */
                }
                //CODE_CALLCRM_HTTPSERVICE      = EPNCODE_TRADE_RESULTCODE + 32; //调用HTTP服务错误
            }
            else //调用CRM错误
            {
                //CERR_CALL_CRMSERVICE       = EPNCODE_TDATA_INFO + 11; //调crmservice错误
            }
            //log theLogger->alert("callSvc error: [trade_id=%ld, user_id=%ld, result_code=%s, result_info=%s]",
            //    m_pTrade->m_tradeid,m_pTrade->m_userid,m_errMsg.c_str(),m_crmTradeItf.getResultInfo());
            return ABMPROMPT_ERROR;
        }
    }
    else if(ABM_PROMPT_APP_TYPE_GRPSTS==m_strAppType)
    {
        char reginStr[10];
        snprintf(reginStr,sizeof(reginStr),"0%d",m_imsNtfGrpsts.get_regionCode());
        AISTD string retInfo;
        AISTD string staff_id("CREDIT00");
        AISTD string depart_id("CREDI");
        if (m_imsNtfGrpsts.get_notificationType()== AREA_STOP_NOTIFY_TYPE)
        {
            staff_id = "area";
            depart_id = "boss";
        }

        //手工信控停机员工ID
        AISTD string strStaffId = m_imsNtfGrpsts.get_staffId();
        LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> STAFF ID:%s", strStaffId.c_str());

        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendStsToCrm(m_svcURI,
                                                    m_crmHttpServiceName,
                                                    "",
                                                    "",
                                                    string(reginStr),
                                                    m_imsNtfGrpsts.get_extend3(),
                                                    staff_id,// "CREDIT00",
                                                    depart_id,//"CREDI",
                                                    m_imsNtfGrpsts.get_resourceId(),
                                                    m_imsNtfGrpsts.get_phoneId(),
                                                    m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(),
                                                    CALL_CRM_SEQUECE_BASE + m_imsNtfGrpsts.get_queueId(),//sequece_id
                                                    "00",  //网别统一填00,没有铁通
                                                    m_imsNtfGrpsts.get_remark(),
                                                    sRemoteAddr,
                                                    sLoginIp,
                                                    sClientIp,
                                                    strStaffId,
                                                    retInfo);
        CALL_CRM_RETRY_END

        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);
        //751013:业务受理前条件判断-用户有未完工的限制业务【缴费复机】！\n214325●办理[7302]业务，用户有未完工的该业务，不需要再处理！\n
        //CRM_COMM_982:用户有未完工的订单[集团信控恢复][3296102897270047][2016-10-28 00:52:28]请稍后！
        //CRM_USER_1013:用户不是欠费预销号状态，不能进行缴费复机操作！
        //115003:办理[7301]业务，用户原服务状态不满足[不能生成服务状态台帐]！\n
        //CRM_TRADE_9:台帐表中已存在对该用户的注销工单，信控已无含义！
        //CRM_GRP_96:该集团已经是正常状态！
        //CRM_USER_15:CRM中用户已处于正常状态，不需要再恢复！
        //CRM_USER_1013:用户不是欠费预销号状态，不能进行缴费复机操作！

        //CRM_TRADE_88:该用户台账表中有其他工单 暂时不能受理！
        //CRM_BOF_014:根据当前TRADE_TYPE_CODE找不到符合条件的BuilderRequestData
        //115999:没有获取到业务类型[null]对应的服务状态变更参数！用户编号[3114070340105357]\n
        //CRM_BIZ_654:该服务号码[13789214370]路由信息不存在
        //CRM_BOF_001:无法获取TradeType数据   TRADE_TYPE_CODE=%
        //CRM_TRADE_325:获取业务类型无数据，业务类型[7301]

        if (m_strCallCrmResultCode == "0")
        {
            return ABMPROMPT_OK;
        }
        else if (m_retryCode.find(m_strCallCrmResultCode) != m_retryCode.end())
        {
            return ABMPROMPT_SLOWSPEED;
            //CODE_CALLCRM_SLOWSPEED        = EPNCODE_TRADE_RESULTCODE + 1; //CRM侧返回需要降速，CRM错误码：CRM_TRADECREDIT_999
            //return 2;
        }
        else if(m_strCallCrmResultCode == "CRM_TRADE_115040" ||
            m_strCallCrmResultCode == "CRM_TRADE_115041" ||
            m_strCallCrmResultCode == "CRM_TRADE_115042" ||
            m_strCallCrmResultCode == "115003" ||
            m_strCallCrmResultCode == "214325" ||
            m_strCallCrmResultCode == "214327" ||
            m_strCallCrmResultCode == "CRM_TRADE_912030" ||
            m_strCallCrmResultCode == "751013" ||
            m_strCallCrmResultCode == "CRM_COMM_982" ||
            m_strCallCrmResultCode == "CRM_TRADE_7")
        {
            return ABMPROMPT_REPEATED;
            //办理[7110]业务，用户状态已经是该状态或存在未完工的
            //变更到该状态的工单，不需要再开通！用户编号[3110110325670010]服务编码[0]状态[7]
            if (m_strCallCrmResultCode == "CRM_TRADE_115040"||m_strCallCrmResultCode == "CRM_TRADE_115041"||m_strCallCrmResultCode == "CRM_TRADE_115042" || m_strCallCrmResultCode == "CRM_TRADE_7")
            {
                //CODE_CALLCRM_SAMETRADE        = EPNCODE_TRADE_RESULTCODE + 3; //CRM侧台帐表中已存在该业务工单，多次发重复工单，CRM侧只接收第一笔，后续工单拒绝接收，CRM错误码：214325
            }
            else if (m_strCallCrmResultCode == "214325")
            {
                //CODE_CALLCRM_HASSTATECHANGE   = EPNCODE_TRADE_RESULTCODE + 2; //CRM侧用户已处于该业务类型状态，不需要再开通，CRM错误码：CRM_TRADE_115040
            }
            else if (m_strCallCrmResultCode == "CRM_TRADE_912030")
            {
                //CODE_CALLCRM_NOTCTRLPROD      = EPNCODE_TRADE_RESULTCODE + 4; //CRM不信控产品
            }
            else
            {
                //CODE_CALLCRM_DIRECTFINISH     = EPNCODE_TRADE_RESULTCODE + 5; //CRM不需要处理
            }
            //log direct finish
        }
        else
        {

            if (m_strCallCrmResultCode == "-100")     //CRM返回字符串解析错误
            {
                //CODE_CALLCRM_DESERIALIZE      = EPNCODE_TRADE_RESULTCODE + 31; //CRM返回字符串解析错误
            }
            else if (m_strCallCrmResultCode == "-101" || m_strCallCrmResultCode == "-102")
            {

                if (m_strCallCrmResultCode == "-101")     //调用服务失败，自动延迟调用三次，超过三次，再置错误
                {
                    /* 调crm接口自动重复调用
                    int t_recallCount = atoi(m_pTrade->m_manageTag.substr(9,1).c_str());
                    if (t_recallCount < 3)  //相隔5分钟，最多重复执行3次
                    {
                        char t_tagBuff[10];
                        sprintf(t_tagBuff,"%s%d",m_pTrade->m_manageTag.substr(0,9).c_str(),t_recallCount+1);
                        m_pCdr->m_manageTag = t_tagBuff;
                        m_pCdr->m_delayInterval = 5; //延迟5分钟
                        return 0;
                    }
                    */
                }
                //CODE_CALLCRM_HTTPSERVICE      = EPNCODE_TRADE_RESULTCODE + 32; //调用HTTP服务错误

            }
            else   //调用CRM错误
            {
                //CERR_CALL_CRMSERVICE     = EPNCODE_TDATA_INFO + 11; //调crmservice错误
            }
            //log theLogger->alert("callSvc error: [trade_id=%ld, user_id=%ld, result_code=%s, result_info=%s]",
            //    m_pTrade->m_tradeid,m_pTrade->m_userid,m_errMsg.c_str(),m_crmTradeItf.getResultInfo());
            return ABMPROMPT_ERROR;
        }
    }
    else if (ABM_PROMPT_APP_TYPE_SERVICE == m_strAppType)
    {
        char reginStr[10];
        snprintf(reginStr, sizeof(reginStr), "0%d", m_imsNtfService.get_regionCode());
        AISTD string retInfo;
        AISTD string strOperStr = m_sysNotificationActionExtcrmIter->second.GetOperCode();
        const AISTD string operStr = ::trimBoth(strOperStr);

        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendServiceToCrm(m_svcURI,
                                                              m_sysNotificationActionExtcrmIter->second.GetCallServiceType(),
                                                              m_crmHttpServiceName,
                                                              string(reginStr),
                                                              "0000",
                                                              "credit",
                                                              "credi",
                                                              m_imsNtfService.get_resourceId(),
                                                              m_imsNtfService.get_phoneId(),
                                                              m_sysNotificationActionExtcrmIter->second.GetElementId(),
                                                              m_sysNotificationActionExtcrmIter->second.GetInModeCode(),
                                                              operStr,
                                                              m_sysNotificationActionExtcrmIter->second.GetServType(),
                                                              m_sysNotificationActionExtcrmIter->second.GetSendFlag(),
                                                              m_sysNotificationActionExtcrmIter->second.GetOpenCode(),
                                                              m_imsNtfService.get_extend2(),
                                                              m_imsNtfService.get_amount(),
                                                              m_imsNtfService.get_credit(),
                                                              m_imsNtfService.get_queueId(),
                                                              m_dtDateTime.toString("%Y%M%D%H%N%S"),
                                                              "",
                                                              m_imsNtfService.get_extend3(),
                                                              m_imsNtfService.get_notifContent(),
                                                              m_sysNotificationActionExtcrmIter->second.GetRsrvStr1(),
                                                              sRemoteAddr,
                                                              sLoginIp,
                                                              sClientIp,
                                                              retInfo,
                                                              m_imsNtfService.get_createDate().toString("%Y%M%D%H%N%S"),
                                                              m_sysNotificationActionExtcrmIter->second.GetRsrvStr3());
        CALL_CRM_RETRY_END

        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);
        if (m_strCallCrmResultCode != "CRM_PARAM_452" && m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(12344566, "\n==[ABMPROMPT::call_crmService]======= ims_ntf_service call http service error!========");
            return -1;
        }
    }
    else if (ABM_PROMPT_APP_TYPE_CONFIRM == m_strAppType)
    {
        AISTD string retInfo;
        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendConfirmToCrm(m_svcURI,
                                                              m_crmHttpServiceName,
                                                              m_dtDateTime.toString("%Y%M%D%H%N%S"),
                                                              m_notifCont.tradeEparchyCode,
                                                              m_notifCont.tradeCityCode,
                                                              m_notifCont.assignStaffid,
                                                              m_notifCont.assignDepartid,
                                                              m_notifCont.updateDepartid,
                                                              m_imsNtfConfirm.get_seriesId(),
                                                              m_notifCont.controlType,
                                                              sRemoteAddr,
                                                              sLoginIp,
                                                              sClientIp,
                                                              retInfo);
        CALL_CRM_RETRY_END
        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);

        if (m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>ims_ntf_confirm call ESOP http service error!========");
            return ABMPROMPT_ERROR;
        }
    }
    else if (ABM_PROMPT_APP_TYPE_TOREAD == m_strAppType)
    {
        AISTD string retInfo;
        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendReadToCrm(m_svcURI,
                                                           m_crmHttpServiceName,
                                                           m_imsNtfToread.get_extend1(),      //把代办主题配置在扩展字段1
                                                           m_imsNtfToread.get_notifContent(), //代办内容
                                                           m_imsNtfToread.get_extend2(),      //把URL链接配置在扩展字段2
                                                           m_imsNtfToread.get_phoneId(),      //客户经理编号
                                                           sRemoteAddr,
                                                           sLoginIp,
                                                           sClientIp,
                                                           retInfo);
        CALL_CRM_RETRY_END
        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);

        if (m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>ims_ntf_confirm call ESOP http service error!========");
            return ABMPROMPT_ERROR;
        }
    }
    else if (ABM_PROMPT_APP_TYPE_CREDIT == m_strAppType)
    {
        AISTD string retInfo;
        CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
        char eparchyCode[5];
        sprintf(eparchyCode, "%04d", m_imsNtfCredit.get_regionCode());
        char recv4[17];
        sprintf(recv4, "%d", m_imsNtfCredit.get_amount());
        AISTD string sourceDb;
        getSourceDbFromRegionCode(string(eparchyCode), sourceDb);
        AISTD string smsKindCode;
        getSmsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode(), smsKindCode);
        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendCreditToCrm(m_svcURI,
                                                             m_crmHttpServiceName,
                                                             TI_O_SMS_SEQUECE_BASE + m_imsNtfCredit.get_queueId(),
                                                             string(eparchyCode),
                                                             "0",
                                                             m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode(),
                                                             2,
                                                             m_sysNotificationActionExtsmsIter->second.GetSendTimeCode(),
                                                             1,
                                                             "00",
                                                             m_imsNtfCredit.get_phoneId(),
                                                             m_imsNtfCredit.get_resourceId(),
                                                             "00",        //m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode(),
                                                             smsKindCode, //m_sysNotificationActionExtsmsIter->second.GetSmsKindCode(),
                                                             "0",
                                                             m_imsNtfCredit.get_notifContent(),
                                                             0,
                                                             1,
                                                             "10086", //"",
                                                             "",
                                                             "",
                                                             m_sysNotificationActionExtsmsIter->second.GetSmsPriority(),
                                                             m_dtDateTime.toString("%Y%M%D%H%N%S"),
                                                             "CREDIT00",
                                                             "CREDI",
                                                             dealTime.toString("%Y%M%D%H%N%S"),
                                                             "",
                                                             "",
                                                             "15",
                                                             "",
                                                             "",
                                                             "",
                                                             "",
                                                             string(recv4),
                                                             atoi(m_dtDateTime.toString("%M").c_str()),
                                                             atoi(m_dtDateTime.toString("%D").c_str()),
                                                             "",
                                                             m_imsNtfCredit.get_templateId(),
                                                             m_sysNotificationActionExtsmsIter->second.GetRsrvStr1(),
                                                             m_imsNtfCredit.get_extend10(),
                                                             sourceDb,
                                                             sRemoteAddr,
                                                             sLoginIp,
                                                             sClientIp,
                                                             retInfo);

        CALL_CRM_RETRY_END
        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);

        if (m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>ims_ntf_credit call ESOP http service error!========");
            return ABMPROMPT_ERROR;
        }
    }
    else if(ABM_PROMPT_APP_TYPE_GRPCREDIT == m_strAppType)
	{
		AISTD string retInfo;
		CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
		char eparchyCode[5];
		sprintf(eparchyCode, "%04d", m_imsNtfCredit.get_regionCode());
		char recv4[17];
		sprintf(recv4, "%d", m_imsNtfCredit.get_amount());
		AISTD string sourceDb;
		getSourceDbFromRegionCode(string(eparchyCode), sourceDb);
		AISTD string smsKindCode;
		getSmsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode(), smsKindCode);
		CALL_CRM_RETRY_BEGIN
		m_strCallCrmResultCode = m_crmCaller.sendCreditToCrm(m_svcURI,
		                                                     m_crmHttpServiceName,
		                                                     TI_O_SMS_SEQUECE_BASE + m_imsNtfCredit.get_queueId(),
		                                                     string(eparchyCode),
		                                                     "0",
		                                                     m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode(),
		                                                     2,
		                                                     m_sysNotificationActionExtsmsIter->second.GetSendTimeCode(),
		                                                     1,
		                                                     "00",
		                                                     m_imsNtfCredit.get_phoneId(),
		                                                     m_imsNtfCredit.get_resourceId(),
		                                                     "00",//m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode(),
		                                                     smsKindCode,//m_sysNotificationActionExtsmsIter->second.GetSmsKindCode(),
		                                                     "0",
		                                                     m_imsNtfCredit.get_notifContent(),
		                                                     0,
		                                                     1,
		                                                     "10086",//"",
		                                                     "",
		                                                     "",
		                                                     m_sysNotificationActionExtsmsIter->second.GetSmsPriority(),
		                                                     m_dtDateTime.toString("%Y%M%D%H%N%S"),
		                                                     "CREDIT00",
		                                                     "CREDI",
		                                                     dealTime.toString("%Y%M%D%H%N%S"),
		                                                     "",
		                                                     "",
		                                                     "15",
		                                                     "",
		                                                     "",
		                                                     "",
		                                                     "",
		                                                     string(recv4),
		                                                     atoi(m_dtDateTime.toString("%M").c_str()),
		                                                     atoi(m_dtDateTime.toString("%D").c_str()),
		                                                     "",
		                                                     m_imsNtfCredit.get_templateId(),
		                                                     m_sysNotificationActionExtsmsIter->second.GetRsrvStr1(),
		                                                     m_imsNtfCredit.get_extend10(),
		                                                     sourceDb,
		                                                     sRemoteAddr,
                                                    	     sLoginIp,
                                                    	     sClientIp,
		                                                     retInfo);

		CALL_CRM_RETRY_END
		if (m_strCallCrmResultCode == "")
		{
			m_strCallCrmResultCode = "-103";
		}
		cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
		cErrorMsg.set_hint(retInfo);
		
		if (m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>ims_ntf_credit call ESOP http service error!========");
            return ABMPROMPT_ERROR;
        }
	}
    else if (ABM_PROMPT_APP_TYPE_REMIND == m_strAppType)
    {
        AISTD string retInfo;
        CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
        char eparchyCode[5];
        sprintf(eparchyCode, "%04d", m_imsNtfRemind.get_regionCode());
        string staff_id("remind");
        if (m_imsNtfRemind.get_notifChannelId() == m_pCfg->m_cfgCommon.m_iNotifyChann)
        {
            staff_id = "wechat";
        }
        AISTD string sourceDb;
        getSourceDbFromRegionCode(string(eparchyCode), sourceDb);
        AISTD string smsKindCode;
        getSmsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode(), smsKindCode);

		
        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendRemindToCrm(m_svcURI,
                                                             m_crmHttpServiceName,
                                                             TI_O_SMS_SEQUECE_BASE + m_imsNtfRemind.get_queueId(),
                                                             string(eparchyCode),
                                                             "0",
                                                             m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode(),
                                                             2,
                                                             m_sysNotificationActionExtsmsIter->second.GetSendTimeCode(),
                                                             1,
                                                             "00",
                                                             m_imsNtfRemind.get_phoneId(),
                                                             m_imsNtfRemind.get_resourceId(),
                                                             "00",        //m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode(),
                                                             smsKindCode, //m_sysNotificationActionExtsmsIter->second.GetSmsKindCode(),
                                                             "0",
                                                             m_imsNtfRemind.get_notifContent(),
                                                             0,
                                                             1,
                                                             "10086", //m_imsNtfRemind.get_extend1(),
                                                             "",
                                                             "",
                                                             m_sysNotificationActionExtsmsIter->second.GetSmsPriority(),
                                                             m_dtDateTime.toString("%Y%M%D%H%N%S"),
                                                             staff_id,
                                                             "CREDI",
                                                             dealTime.toString("%Y%M%D%H%N%S"),
                                                             "",
                                                             "",
                                                             "15",
                                                             "",
                                                             "",
                                                             "",
                                                             "",
                                                             "",
                                                             atoi(m_dtDateTime.toString("%M").c_str()),
                                                             "",
                                                             "",
                                                             m_imsNtfRemind.get_templateId(),
                                                             m_sysNotificationActionExtsmsIter->second.GetRsrvStr1(),
                                                             "",
                                                             sourceDb,
                                                             sRemoteAddr,
                                                             sLoginIp,
                                                             sClientIp,
                                                             retInfo,
                                                             atoi(m_dtDateTime.toString("%D").c_str()),
                                                             m_imsNtfRemind.get_extend8());

        CALL_CRM_RETRY_END
        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);

        if (m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>ims_ntf_remind call ESOP http service error!========");
            return ABMPROMPT_ERROR;
        }
    }
    else
    {
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> func: %s, app_type = %s undefined ,please check confiugre file!", __func__, m_strAppType.c_str());
    }
    return ABMPROMPT_OK;
}

int32 ABMPrompt::call_crmService_QH(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    if (get_svcURI(pSession, cErrorMsg) != ABMPROMPT_OK)
    {
        LOG_ERROR(ABMPROMPT_XC_ERROR, "====[ABM_SERV_ABMPROMPT]===> get_svcURI failed");
        return ABMPROMPT_ERROR;
    }

    //当模拟调crm时，用耗时usleep来模拟调用时间
    if (PROMPT_NO_CALL_CRM == m_pCfg->m_cfgCommon.m_nNoCallCrm)
    {
        // usleep 参数单位为us，1s = 10^6 us
        usleep(static_cast<uint32>(m_pCfg->m_cfgCommon.m_dSimSleep * 1000000));
        cErrorMsg.set_hint("sleep simulate call CRM!");
        cErrorMsg.set_errorMsg("NO call CRM");
        LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===>sleep is used to simulate calling crm , elapsed %f s", m_pCfg->m_cfgCommon.m_dSimSleep);
        return ABMPROMPT_OK;
    }

    if (ABM_PROMPT_APP_TYPE_STS == m_strAppType)
    {
        AISTD string userRgCode = m_imsNtfSts.get_extend3();
        if (userRgCode.empty())
        {
            char reginStr[10];
            snprintf(reginStr, sizeof(reginStr), "%04d", m_imsNtfSts.get_regionCode());
            userRgCode = string(reginStr);
        }
        AISTD string retInfo;
        //特停特开等业务，工号需要输入营业工号，通过extend1字段传入，如果字段被占用，此处需要修改
        AISTD string staff_id("CREDIT00");
        AISTD string depart_id("CREDI");
        AISTD string t_staffInfo = m_imsNtfSts.get_extend1();
        if (!t_staffInfo.empty())
        {
            vector<AISTD string> t_vStaff;
            t_vStaff.clear();
            cdk::strings::Split(t_staffInfo, ",", t_vStaff);
            if (t_vStaff.size() >= 2)
            {
                if (!t_vStaff[0].empty())
                {
                    staff_id = t_vStaff[0];
                }
                if (!t_vStaff[1].empty())
                {
                    depart_id = t_vStaff[1];
                }
            }
        }

        long lLeaveRealFee = m_imsNtfSts.get_amount();                     //计算结余
        long lRealFee = cdk::strings::Atol64(m_imsNtfSts.get_notifContent().c_str());      //字段notif_content存放实时费用
        long lTradeId = CALL_CRM_SEQUECE_BASE + m_imsNtfSts.get_queueId(); //trade_id

        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendStsToCrm(m_svcURI,
                                                          m_crmHttpServiceName,
                                                          string(m_bomcFile.getTraceId()),
                                                          string(m_bomcFile.getUuid()),
                                                          userRgCode,
                                                          "",        //CITY_CODE
                                                          staff_id,  // "CREDIT00",
                                                          depart_id, //"CREDI",
                                                          m_imsNtfSts.get_resourceId(),
                                                          m_imsNtfSts.get_phoneId(),
                                                          m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(),
                                                          lTradeId,
                                                          "00", //网别统一填00,没有铁通
                                                          m_imsNtfSts.get_remark(),
                                                          lLeaveRealFee,
                                                          m_imsNtfSts.get_credit(),
                                                          lRealFee,
                                                          retInfo);
        CALL_CRM_RETRY_END

        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);

        //返回值"0"调用成功,"-1"调用失败
        if (m_strCallCrmResultCode == "0")
        {
            return ABMPROMPT_OK;
        }
        else if (m_retryCode.find(m_strCallCrmResultCode) != m_retryCode.end())
        {
            return ABMPROMPT_SLOWSPEED;
            //CODE_CALLCRM_SLOWSPEED        = EPNCODE_TRADE_RESULTCODE + 1; //CRM侧返回需要降速，CRM错误码：CRM_TRADECREDIT_999
            //return 2;
        }
        //-100 字符串解析错，-101 调CRM 服务错，-102 调用HTTP服务错
        else if ((m_strCallCrmResultCode == "-100") || (m_strCallCrmResultCode == "-101") || (m_strCallCrmResultCode == "-102"))
        {
            return ABMPROMPT_ERROR;
        }
        //用户已处于该业务类型状态，不需要再开通，错误码：CRM_TRADE_115040
        //多次发重复工单，CRM只接收第一笔，后续工单拒绝接收，错误码：CRM_TRADE_115040
        //铁通无线固话：CRM_TRADE_115041
        // 宽带半停机：CRM_TRADE_115042
        //BBOSS报文已发 错误码：400000
        //用户存在购机业务或是一卡双号用户不能做销号的规则，错误码：214327
        //CRM集团产品暂时不支持该信控业务，错误码：CRM_TRADE_912030
        //else if ((m_strCallCrmResultCode == "CRM_TRADE_115040") || (m_strCallCrmResultCode == "214325") || (m_strCallCrmResultCode == "400000") || (m_strCallCrmResultCode == "214327") || (m_strCallCrmResultCode == "CRM_TRADE_912030") || (m_strCallCrmResultCode == "115005") || (m_strCallCrmResultCode == "916030") || (m_strCallCrmResultCode == "CRM_TRADE_7"))
        else if((m_strCallCrmResultCode == "CRM_TRADE_115040")||(m_strCallCrmResultCode == "CRM_TRADE_115041")||(m_strCallCrmResultCode == "CRM_TRADE_115042")||(m_strCallCrmResultCode == "CRM_TRADE_912030"))
        {
            LOG_TRACE("callSvc direct finish: [trade_id=%ld, user_id=%lld, result_code=%s, not record tf_b_trade]",
                      lTradeId, m_imsNtfSts.get_resourceId(), m_strCallCrmResultCode.c_str());
            return ABMPROMPT_REPEATED;
        }
        else
        {
            LOG_TRACE("callSvc direct finish: [trade_id=%ld, user_id=%lld, result_code=%s, not record tf_b_trade]",
                      lTradeId, m_imsNtfSts.get_resourceId(), m_strCallCrmResultCode.c_str());
            return ABMPROMPT_ERROR;
        }
    }
    else if (ABM_PROMPT_APP_TYPE_SERVICE == m_strAppType)
    {
        char reginStr[10];
        snprintf(reginStr, sizeof(reginStr), "%04d", m_imsNtfService.get_regionCode());
        AISTD string retInfo;

        vector<string> vct_extend2;
        map<string, string> map_extend2;
        vector<string> key_value;
        cdk::strings::Split(m_imsNtfService.get_extend2(), ";", vct_extend2);
        for (vector<string>::iterator itr = vct_extend2.begin(); itr != vct_extend2.end(); itr++)
        {
            cdk::strings::Split(*itr, "|", key_value);
            if (key_value.size() == 2)
            {
                map_extend2.insert(map<string, string>::value_type(::trimBoth(key_value[0]), ::trimBoth(key_value[1])));
            }
        }
        AISTD string strDiscntCode;
        AISTD string strThreshold;
        AISTD string strLimitValue;
        if (map_extend2.find("5035") != map_extend2.end())
        {
            strDiscntCode = map_extend2["5035"];
        }
        if (map_extend2.find("5002") != map_extend2.end())
        {
            strThreshold = map_extend2["5002"];
        }
        if (map_extend2.find("5022") != map_extend2.end())
        {
            strLimitValue = map_extend2["5022"];
        }

        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendServiceToCrm(m_svcURI,
                                                              m_sysNotificationActionExtcrmIter->second.GetCallServiceType(),
                                                              m_crmHttpServiceName,
                                                              string(reginStr),
                                                              "0000",
                                                              "credit",
                                                              "credi",
                                                              m_imsNtfService.get_resourceId(),
                                                              m_imsNtfService.get_phoneId(),
                                                              m_sysNotificationActionExtcrmIter->second.GetElementId(),
                                                              m_sysNotificationActionExtcrmIter->second.GetInModeCode(),
                                                              m_sysNotificationActionExtcrmIter->second.GetOperCode(),
                                                              m_sysNotificationActionExtcrmIter->second.GetServType(),
                                                              m_sysNotificationActionExtcrmIter->second.GetSendFlag(),
                                                              m_sysNotificationActionExtcrmIter->second.GetOpenCode(),
                                                              m_dtDateTime.toString("%Y%M%D%H%N%S"),
                                                              m_sysNotificationActionExtcrmIter->second.GetRsrvStr3(),
                                                              retInfo,
                                                              strDiscntCode,
                                                              strThreshold,
                                                              strLimitValue,
                                                              m_imsNtfService.get_queueId(),
                                                              m_sysNotificationActionExtcrmIter->second.GetRsrvStr1(),
                                                              m_imsNtfService.get_amount(),
                                                              m_imsNtfService.get_notifContent());
        CALL_CRM_RETRY_END

        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);
        if (m_strCallCrmResultCode != "CRM_PARAM_452" && m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(12344566, "\n==[ABMPROMPT::call_crmService]======= ims_ntf_service call http service error!========");
            return ABMPROMPT_ERROR;
        }
    }
    else if (ABM_PROMPT_APP_TYPE_CONFIRM == m_strAppType)
    {
        AISTD string retInfo;
        //从拓展1字段中取值
        vector<string> vct_extend1;
        map<string, string> map_extend1;
        vector<string> key_value;
        cdk::strings::Split(m_imsNtfConfirm.get_extend1(), ";", vct_extend1);
        for (vector<string>::iterator itr = vct_extend1.begin(); itr != vct_extend1.end(); itr++)
        {
            cdk::strings::Split(*itr, "|", key_value);
            if (key_value.size() == 2)
            {
                map_extend1.insert(map<string, string>::value_type(::trimBoth(key_value[0]), ::trimBoth(key_value[1])));
            }
        }
        AISTD string strGroupType;
        if (map_extend1.find("5333") != map_extend1.end())
        {
            strGroupType = map_extend1["5333"];
        }

        CALL_CRM_RETRY_BEGIN
        m_strCallCrmResultCode = m_crmCaller.sendConfirmToCrm(m_svcURI,
                                                              m_crmHttpServiceName,
                                                              m_dtDateTime.toString("%Y%M%D%H%N%S"),
                                                              m_notifCont.tradeEparchyCode,
                                                              m_notifCont.tradeCityCode,
                                                              m_notifCont.assignStaffid,
                                                              m_notifCont.assignDepartid,
                                                              m_notifCont.updateDepartid,
                                                              m_imsNtfConfirm.get_seriesId(),
                                                              m_notifCont.controlType,
                                                              strGroupType,
                                                              m_imsNtfConfirm.get_extend3(),
                                                              retInfo);
        CALL_CRM_RETRY_END
        if (m_strCallCrmResultCode == "")
        {
            m_strCallCrmResultCode = "-103";
        }
        cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
        cErrorMsg.set_hint(retInfo);

        if (m_strCallCrmResultCode != "0")
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>ims_ntf_confirm call ESOP http service error!========");
            return ABMPROMPT_ERROR;
        }
    }
    else
    {
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> func: %s, app_type = %s undefined ,please check confiugre file!", __func__, m_strAppType.c_str());
    }
    return ABMPROMPT_OK;
}

int32 ABMPrompt::get_limitSpeed(SOBSession *pSession, CBSErrorMsg &cErrorMsg, AISTD string &r_limitSpeed)
{
    m_crmHttpServiceName = m_crmHttpQueryLimitSpeedName;
    AISTD string retInfo;
    if (get_svcURI(pSession, cErrorMsg) != ABMPROMPT_OK)
    {
        LOG_ERROR(ABMPROMPT_XC_ERROR, "====[ABM_SERV_ABMPROMPT]===> get_svcURI failed");
        return ABMPROMPT_ERROR;
    }
    AISTD string t_SpeedRatio = "";
    AISTD string t_SpeedRemark;
    int32 i_SpeedRatio;
    char reginStr[10];
    snprintf(reginStr, sizeof(reginStr), "%04d", m_imsNtfRemind.get_regionCode());
    CALL_CRM_RETRY_BEGIN
    m_strCallCrmResultCode = m_crmCaller.sendLimitSpeedToCrm(m_svcURI,
                                                             m_crmHttpServiceName,
                                                             string(reginStr),
                                                             m_imsNtfRemind.get_resourceId(),
                                                             m_imsNtfRemind.get_phoneId(),
                                                             retInfo,
                                                             t_SpeedRatio,
                                                             t_SpeedRemark);
    CALL_CRM_RETRY_END
    if (m_strCallCrmResultCode == "0")
    {
        if (!t_SpeedRatio.empty())
        {
        	r_limitSpeed = t_SpeedRatio + "Mbps";		
        }
        else
        {
            r_limitSpeed = "1Mbps";
        }
    }
    else
    {
    	cErrorMsg.set_errorMsg(m_strCallCrmResultCode);//add by sunjz 20210331
        cErrorMsg.set_hint(retInfo);
        return ABMPROMPT_ERROR;
    }
    return ABMPROMPT_OK;
}

//调CRM平台接口，获取附加的短信
int ABMPrompt::call_crmExternSms(SOBSession *pSession,
                                 const AISTD string &r_serialNumber,
                                 AISTD string &r_externSms,
                                 CBSErrorMsg &cErrorMsg)
{
    //m_crmHttpServiceName = "SS.SmsTempletManagerSVC.getSms";
    m_crmHttpServiceName = m_pCfg->m_cfgCommon.m_szRemindServName;

    if (get_svcURI(pSession, cErrorMsg) != ABMPROMPT_OK)
    {
        LOG_ERROR(ABMPROMPT_XC_ERROR, "====[ABM_SERV_ABMPROMPT]===> get_svcURI failed");
        return ABMPROMPT_ERROR;
    }

    char reginStr[10];
    snprintf(reginStr, sizeof(reginStr), "%04d", m_imsNtfRemind.get_regionCode());
    AISTD string strChnlType = AISTD string(m_pCfg->m_cfgParams.m_strChnlType);
    AISTD string strSaleActCode = AISTD string(m_pCfg->m_cfgParams.m_strSaleActCode);
    AISTD string strStaffId = m_sysNotificationActionExtsmsIter->second.GetRsrvStr2();

    CALL_CRM_RETRY_BEGIN
    //调用crm接口，获取催费的附加短信
    m_strCallCrmResultCode = m_crmCaller.getSmsFromCrm(m_svcURI,
                                                       m_crmHttpServiceName,
                                                       r_serialNumber,
                                                       strChnlType,
                                                       strSaleActCode,
                                                       AISTD string(reginStr),
                                                       strStaffId,
                                                       r_externSms);
    CALL_CRM_RETRY_END

    if (m_strCallCrmResultCode != "0")
    {
        LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>sms call  http service error!========");
        return ABMPROMPT_ERROR;
    }
    return ABMPROMPT_OK;
}

int32 ABMPrompt::get_actionExtInfo(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    //以下获取action_id对应的action_type
    TRY_BEGIN
    ENTER_FUNC
    xc::CSnapshot cSnap("ZW::ABM_BALANCE");
    xc::CQueryHolder<ZW::ABM_BALANCE::CNotifydealSysNotificationAction::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotifydealSysNotificationAction::GetContainerName());
    ZW::ABM_BALANCE::CNotifydealSysNotificationAction::Type::iterator iter;
    iter = cQueryHolder.GetContainer().find(m_actionId);
    if (iter == cQueryHolder.GetContainer().end())
    {
        LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>  can't lookup action_id = %d  from NOTIFYDEAL_SYS_NOTIFICATION_ACTION", m_actionId);
        return ABMPROMPT_XC_ERROR;
    }
    else
    {
        m_actionType = iter->second.GetActionType();
    }

    //以下获取action_id对应的ext信息
    //if (ACTION_TYPE_SMS_REMIND == m_actionType || ACTION_TYPE_SMS_CREDIT == m_actionType || ACTION_TYPE_SMS_BATCHREMIND == m_actionType || ACTION_TYPE_SMS_REMIND_ONTIME == m_actionType)
    if(ABM_PROMPT_APP_TYPE_CREDIT == m_strAppType || ABM_PROMPT_APP_TYPE_GRPCREDIT == m_strAppType || ABM_PROMPT_APP_TYPE_REMIND == m_strAppType
    	|| ABM_PROMPT_APP_TYPE_REMINDONTIME == m_strAppType || ABM_PROMPT_APP_TYPE_WECHAT == m_strAppType)
    {
        //获取短信类action附加信息
        xc::CSnapshot cSnap("ZW::ABM_BALANCE");
        xc::CQueryHolder<ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtsms::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtsms::GetContainerName());
        m_sysNotificationActionExtsmsIter = cQueryHolder.GetContainer().find(m_actionId);
        if (m_sysNotificationActionExtsmsIter == cQueryHolder.GetContainer().end())
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>  can't lookup action_id = %d  from NOTIFYDEAL_SYS_NOTIFICATION_ACTION_EXTSMS", m_actionId);
            return ABMPROMPT_XC_ERROR;
        }

        // add by ligc@20190418 for 短信夹带
        string str = m_sysNotificationActionExtsmsIter->second.GetRsrvStr1();
        if (!str.empty())
        {
            m_isSmsMarket = true;
            m_strSmsTag=str;
        }
		m_crmHttpServiceName = m_sysNotificationActionExtsmsIter->second.GetRsrvStr2();
    }
    else
    {
        //获取调crm接口类附加信息
        xc::CSnapshot cSnap("ZW::ABM_BALANCE");
        xc::CQueryHolder<ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtcrm::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtcrm::GetContainerName());
        m_sysNotificationActionExtcrmIter = cQueryHolder.GetContainer().find(m_actionId);
        if (m_sysNotificationActionExtcrmIter == cQueryHolder.GetContainer().end())
        {
            LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>  can't lookup action_id = %d  from NOTIFYDEAL_SYS_NOTIFICATION_ACTION_EXTCRM", m_actionId);
            return ABMPROMPT_XC_ERROR;
        }
        m_crmHttpServiceName = m_sysNotificationActionExtcrmIter->second.GetCallServiceName();
    }
    CATCH_END
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::get_svcURI(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    TRY_BEGIN
    ENTER_FUNC

    //以下获取服务对应的地址
    xc::CSnapshot cSnap("ZW::ABM_BALANCE");
    xc::CQueryHolder<ZW::ABM_BALANCE::CAbmSysParameter::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CAbmSysParameter::GetContainerName());
    //根据工单要调用的接口名和PARAM_CODE从sys_parameter中查询要调用的地址
    aistring strServiceAddrParamCode = "ABM_PROMPT_CALL_SERVICE_ADDR";
    bool bFound = false;
    std::pair<ZW::ABM_BALANCE::CAbmSysParameter::Type::iterator, ZW::ABM_BALANCE::CAbmSysParameter::Type::iterator> keyPair;
    keyPair = cQueryHolder.GetContainer().equal_range(strServiceAddrParamCode);
    for (auto iter = keyPair.first; iter != keyPair.second; ++iter)
    {
        if (m_crmHttpServiceName == iter->second.GetExtClass())
        {
            m_crmHttpAddr = iter->second.GetParamValue();
            bFound = true;
            break;
        }
    }

    if (!bFound)
    {
        LOG_ERROR(ABMPROMPT_XC_ERROR, " ====[ABM_SERV_ABMPROMPT]===> can't lookup m_crmHttpAddr in SysParameter when strServiceAddrParamCode = %s, m_crmHttpServiceName = %s ",
                  strServiceAddrParamCode.c_str(), m_crmHttpServiceName.c_str());
        char info[1024];
        snprintf(info, sizeof(info), "can't lookup m_crmHttpAddr in SysParameter when strServiceAddrParamCode = %s, m_crmHttpServiceName = %s ",
                 strServiceAddrParamCode.c_str(),
                 m_crmHttpServiceName.c_str());
        cErrorMsg.set_hint(info);
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
        return ABMPROMPT_XC_ERROR;
    }

    //按分隔符解析字段，放入uri中
    vector<aistring> addrString;
    addrString.clear();
    cdk::strings::Split(m_crmHttpAddr, ":", addrString);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> parse m_crmHttpAddr:%s, result vector size is %d ...", m_crmHttpAddr.c_str(), addrString.size());
    if (addrString.size() >= 5)
    {
        sprintf(m_svcURI.protocol, "%s", addrString[0].c_str());
        sprintf(m_svcURI.machine, "%s", addrString[1].c_str());
        m_svcURI.port = atoi(addrString[2].c_str());
        sprintf(m_svcURI.serviceLocation, "%s", addrString[3].c_str());
        sprintf(m_svcURI.username, "%s", addrString[4].c_str());
        if (addrString.size() == 6)
        {
            sprintf(m_svcURI.password, "%s", addrString[5].c_str());
        }
    }
    else
    {
        //Todo:
        char info[1024];
        snprintf(info, sizeof(info), "parse %s  failed ,please check!,the strServiceAddrParamCode = %s, m_crmHttpServiceName = %s ",
                 m_crmHttpAddr.c_str(),
                 strServiceAddrParamCode.c_str(),
                 m_crmHttpServiceName.c_str());
        LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===> %s", info);
        cErrorMsg.set_hint(info);
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
        return ABMPROMPT_XC_ERROR;
    }
    CATCH_END
    LEAVE_FUNC
    return 0;
}

int32 ABMPrompt::get_crmService(SOBSession* pSession,const aistring paramCode,CBSErrorMsg& cErrorMsg)
{
	int32 iRet = SDL_OK;
	ENTER_FUNC
	TRY_BEGIN
	xc::CSnapshot cSnap("ZW::ABM_BALANCE");
	xc::CQueryHolder<ZW::ABM_BALANCE::CAbmSysParameter::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CAbmSysParameter::GetContainerName());
	ZW::ABM_BALANCE::CAbmSysParameter::Type::iterator iter;
	iter = cQueryHolder.GetContainer().find(paramCode);
	if (iter != cQueryHolder.GetContainer().end())
	{
		m_crmHttpServiceName = iter->second.GetParamValue();
		LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===>%s = %s ... ", paramCode.c_str(), m_crmHttpServiceName.c_str());
	}
	else
	{
		iRet = SDL_FAIL;
		LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===>can't find  %s in  DEDUCT_SYS_PARAMETER ", paramCode.c_str());
	}
	CATCH_END
	LEAVE_FUNC
	return iRet;
}

int32 ABMPrompt::process_credit(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit->query_data")
    query_data(m_listImsNtfCredit);
    ES_END_RUN_TIME

    //没有捞到数据则休眠一段时间，休眠时间默认配置为5秒
    if (m_listImsNtfCredit.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfCredit is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_credit order : %d orders from table %s.", m_listImsNtfCredit.size(), m_sourceTable.c_str());

    aistring strtmpValue(""); //add by fsl@20191115 for 用户全生命周期信控短信提醒
    get_sysParamter("USER_OWE_FEE_FLAG", strtmpValue);
    vector<aistring> triggeringNotificationIDList;
    triggeringNotificationIDList.clear();
    cdk::strings::Split(strtmpValue, "|", triggeringNotificationIDList);

    m_listImsNtfCreditHis.clear();
    int32 iCount = 0;
    m_listStatIn.clear();
    m_listStatUp.clear();
    for (m_itrImsNtfCredit = m_listImsNtfCredit.begin(); m_itrImsNtfCredit != m_listImsNtfCredit.end(); m_itrImsNtfCredit++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfCredit = *m_itrImsNtfCredit;
        m_actionId = m_imsNtfCredit.get_actionId();
        m_listImsNtfCreditHis.clear();
        int32 iRetValue = ABMPROMPT_OK;
        m_isSmsMarket = false;
        m_strSmsTag.clear();
        bool bSmsToTable = true;
        bool bIsCallServiceFail = false;
        //if (ENUM_ACTION_TYPE_OVER != m_imsNtfCredit.get_actionType())
        {
            //通过m_actionId获取xc中对应的信控参数
            if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
            {
                AISTD string strErrMsg("action_id = ");
                strErrMsg.append(cdk::strings::Itoa(m_actionId));
                strErrMsg.append("  get action_id info failed");
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
                cErrorMsg.set_hint(strErrMsg);
                iRetValue = ABMPROMPT_ERROR;
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
            }
            //获取工单表IMS_NTF_XX里的短信内容
            if (m_imsNtfCredit.get_notifContent().empty())
            {
                iRetValue = ABMPROMPT_ERROR;
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
                cErrorMsg.set_hint("notif_content is empty");
            }
            //校验手机号码格式是否正确--为11位，且第一位为'1'
            else if (!check_phoneId(m_imsNtfCredit.get_phoneId()))
            {
                iRetValue = ABMPROMPT_ORDER_PHONE;
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ORDER_PHONE));
                cErrorMsg.set_hint("phone_id is inaccurate ,don't need to send message");
            }
            //校验短信内容是否正确--短信模板中的"<>"已被替换完毕
            else if (!check_notifyContent(m_imsNtfCredit.get_notifContent()))
            {
                iRetValue = ABMPROMPT_ERROR;
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ERROR_MESSAGE));
                cErrorMsg.set_hint("the notif_content contains  '<' or '>' ,some param_id don't have be replaced.");
            }

            if (ABMPROMPT_OK == iRetValue)
            {
                m_nActionLevel = m_imsNtfCredit.get_actionLevel();
                if (ENUM_ACTION_LEVEL_APP == m_nActionLevel) //app提醒工单
                {
                    //准备数据
                    m_listTiOFsbdAppmsg.clear();

                    AISTD string strQueueId = ltoa(m_imsNtfCredit.get_queueId());

                    //注意：当工单量每秒超过百万条时，这种batchId拼接方式会出现重复
                    AISTD string strAppBatchId = "ACT" + m_dtDateTime.toString("%Y%M%D%H%N%S") + strQueueId.substr(strQueueId.length() - 6); //账务为ACT+YYYYMMDDHHmmSS+6位序列号
                    int32 iPartitionId = atoi(strAppBatchId.substr(strAppBatchId.length() - 3).c_str());

                    AISTD string strTemplateContent = "";
                    AISTD string strTitle = "";
                    AISTD string strAddressType = "";
                    int32 iAddressType = 0;
                    AISTD string strAddress = "";
                    AISTD string strMsgType = "";
                    int32 iMonth = 0;
                    int32 iDay = 0;
                    int64 llResourceId = m_imsNtfCredit.get_resourceId();
                    char eparchyCode[5];
                    sprintf(eparchyCode, "%04d", m_imsNtfCredit.get_regionCode());
                    AISTD string strRecvObject = m_imsNtfCredit.get_phoneId();

                    iMonth = atoi(m_dtDateTime.toString("%M").c_str());
                    iDay = atoi(m_dtDateTime.toString("%D").c_str());

                    //提醒相关信息由于字段不足拼接在同一个字段中
                    LOG_TRACE("[WXMSG]=======>get_notifContent:%s", m_imsNtfRemind.get_notifContent().c_str());
                    CStringList lstStrWithMsgType;
                    CStringList lstStr;
                    int32 iMsgTypeSplitRet = split_msgType_notifyContent(m_imsNtfCredit.get_notifContent(), "|||", lstStrWithMsgType);
                    if (iMsgTypeSplitRet == 2)
                    {
                        strMsgType = lstStrWithMsgType.at(1);
                        base_divide_str(lstStrWithMsgType.at(0), "|", lstStr);
                        if (lstStr.size() == 3)
                        {
                            strTitle = lstStr.at(0);
                            strTemplateContent = lstStr.at(1);
                            strAddressType = lstStr.at(2);
                        }

                        if (lstStr.size() == 4)
                        {
                            strTitle = lstStr.at(0);
                            strTemplateContent = lstStr.at(1);
                            strAddressType = lstStr.at(2);
                            strAddress = lstStr.at(3);
                        }
                    }
                    else
                    {
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_imsNtfCredit.get_notifContent error");
                    }
                    LOG_TRACE("[WXMSG]=======>strMsgType:%s", strMsgType.c_str());
                    iAddressType = strAddressType == "" ? 0 : atoi(strAddressType.c_str());

                    MAbmInterfacePromptDb::CTiOFsbdAppmsg tiOFsbdAppMsg;
                    tiOFsbdAppMsg.set_appBatchId(strAppBatchId);
                    tiOFsbdAppMsg.set_partitionId(iPartitionId);
                    tiOFsbdAppMsg.set_recvId(llResourceId);
                    tiOFsbdAppMsg.set_recvObject(strRecvObject);
                    tiOFsbdAppMsg.set_messageTitle(strTitle);
                    tiOFsbdAppMsg.set_messageDescription(strTemplateContent);
                    tiOFsbdAppMsg.set_addressType(iAddressType);
                    tiOFsbdAppMsg.set_address(strAddress);
                    tiOFsbdAppMsg.set_isLogin(0);   //是否强制登录，1-是，0-否
	                tiOFsbdAppMsg.set_msgType(strMsgType);//消息类型，3：提醒类
                    tiOFsbdAppMsg.set_sendType(0);  //立即发送
                    tiOFsbdAppMsg.set_pushType(0);  //单播
                    tiOFsbdAppMsg.set_pushLevel(1); //高级
                    tiOFsbdAppMsg.set_endpointId("cmcc1");
                    tiOFsbdAppMsg.set_eparchyCode(eparchyCode);
                    tiOFsbdAppMsg.set_referTime(m_dtDateTime);
                    tiOFsbdAppMsg.set_dealTime(m_dtDateTime);
                    tiOFsbdAppMsg.set_dealState(0);
                    tiOFsbdAppMsg.set_month(iMonth);
                    tiOFsbdAppMsg.set_day(iDay);

                    m_listTiOFsbdAppmsg.push_back(tiOFsbdAppMsg);
                    //入表
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertTiOFsbdAppmsg")
                    iRetValue = insertTiOFsbdAppmsg(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                }
                else if (ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel) //微信提醒工单
                {
                    //准备数据
                    m_listTiOFsbdWxmsg.clear();

                    AISTD string strQueueId = ltoa(m_imsNtfCredit.get_queueId());
                    //注意：当工单量每秒超过百万条时，这种batchId拼接方式会出现重复
                    AISTD string strAppBatchId = "ACT" + m_dtDateTime.toString("%Y%M%D%H%N%S") + strQueueId.substr(strQueueId.length() - 6); //账务为ACT+YYYYMMDDHHmmSS+6位序列号
                    int32 iPartitionId = atoi(strAppBatchId.substr(strAppBatchId.length() - 3).c_str());

                    AISTD string strTemplateId = "";
                    AISTD string strTemplateUrl = "";
                    AISTD string strTemplateContent = "";
                    AISTD string strMsgType = "";
                    int32 iMonth = 0;
                    int32 iDay = 0;
                    int64 llResourceId = m_imsNtfCredit.get_resourceId();
                    AISTD string strRecvObject = m_imsNtfCredit.get_phoneId();
                    char eparchyCode[5];
                    sprintf(eparchyCode, "%04d", m_imsNtfCredit.get_regionCode());

                    iMonth = atoi(m_dtDateTime.toString("%M").c_str());
                    iDay = atoi(m_dtDateTime.toString("%D").c_str());

                    //字段数量不足，提醒内容拼接在notifContent字段种
                    LOG_TRACE("[WXMSG]=======>get_notifContent:%s", m_imsNtfCredit.get_notifContent().c_str());
                    CStringList lstStrWithMsgType;
                    CStringList lstWechatStr;
                    int32 iMsgTypeSplitRet = split_msgType_notifyContent(m_imsNtfCredit.get_notifContent(), "|||", lstStrWithMsgType);
                    if (iMsgTypeSplitRet == 2)
                    {
                        strMsgType = lstStrWithMsgType.at(1);
                        base_divide_str(lstStrWithMsgType.at(0), "|", lstWechatStr);
                        if (lstWechatStr.size() == 2)
                        {
                            strTemplateContent = lstWechatStr.at(0);
                            strTemplateId = lstWechatStr.at(1);
                        }

                        if (lstWechatStr.size() == 3)
                        {
                            strTemplateContent = lstWechatStr.at(0);
                            strTemplateId = lstWechatStr.at(1);
                            strTemplateUrl = lstWechatStr.at(2);
                        }
                    }
                    else
                    {
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_imsNtfCredit.get_notifContent error");
                    }
                    LOG_TRACE("[WXMSG]=======>strMsgType:%s", strMsgType.c_str());

                    MAbmInterfacePromptDb::CTiOFsbdWxmsg tiOFsbdWxmsg;
                    tiOFsbdWxmsg.set_appBatchId(strAppBatchId);
                    tiOFsbdWxmsg.set_partitionId(iPartitionId);
                    tiOFsbdWxmsg.set_recvId(llResourceId);
                    tiOFsbdWxmsg.set_recvObject(strRecvObject);
                    tiOFsbdWxmsg.set_templateId(strTemplateId);
                    tiOFsbdWxmsg.set_msgParams(strTemplateContent);
                    tiOFsbdWxmsg.set_url(strTemplateUrl);
                    tiOFsbdWxmsg.set_msgType(strMsgType);
                    tiOFsbdWxmsg.set_sendType(0);
                    tiOFsbdWxmsg.set_pushType(0);
                    tiOFsbdWxmsg.set_pushLevel(1);
                    tiOFsbdWxmsg.set_eparchyCode(eparchyCode);
                    tiOFsbdWxmsg.set_referTime(m_dtDateTime);
                    tiOFsbdWxmsg.set_dealTime(m_dtDateTime);
                    tiOFsbdWxmsg.set_dealState(0);
                    tiOFsbdWxmsg.set_month(iMonth);
                    tiOFsbdWxmsg.set_day(iDay);

                    m_listTiOFsbdWxmsg.push_back(tiOFsbdWxmsg);

                    //入表
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertTiOFsbdWxmsg")
                    iRetValue = insertTiOFsbdWxmsg(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                }
                else if (m_smsTable.compare(TI_O_SMS)==0)
                {
                    m_listTiOSms.Clear();
                    MAbmInterfacePromptDb::CTiOSms tiOSmsTmp;
                    CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
                    char eparchyCode[5];
                    sprintf(eparchyCode, "%04d", m_imsNtfCredit.get_regionCode());
                    char recv4[17];
                    sprintf(recv4, "%d", m_imsNtfCredit.get_amount());
                    // mod by ligc@20190418 短信夹带
                    if ((!m_crmHttpServiceName.empty()) && (m_isSmsMarket) && (m_isCallSmsService))
                    {
                        bSmsToTable = false;
                    }
                    //if (bSmsToTable&&(m_strSmsTag!="0"))
                    if(m_strSmsTag!="0")//0表示只调消息中心，不发送短信
                    {
                        /*tiOSmsTmp.set_smsNoticeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfCredit.get_queueId());
                        tiOSmsTmp.set_eparchyCode(eparchyCode);
                        tiOSmsTmp.set_inModeCode("0");
                        tiOSmsTmp.set_smsChannelCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
                        tiOSmsTmp.set_sendObjectCode(2);
                        tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                        tiOSmsTmp.set_sendCountCode(1);
                        tiOSmsTmp.set_recvObjectType("00");
                        tiOSmsTmp.set_recvObject(m_imsNtfCredit.get_phoneId());
                        tiOSmsTmp.set_id(m_imsNtfCredit.get_resourceId());
                        tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                        tiOSmsTmp.set_smsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                        tiOSmsTmp.set_noticeContentType("0");
                        tiOSmsTmp.set_noticeContent(m_imsNtfCredit.get_notifContent());
                        //tiOSmsTmp.set_referedCount(const int32 & value);
                        tiOSmsTmp.set_forceReferCount(1);
                        //tiOSmsTmp.set_forceObject(const aistring & value);
                        //tiOSmsTmp.set_forceStartTime(const CBSDateTime & value);
                        //tiOSmsTmp.set_forceEndTime(const CBSDateTime & value);
                        tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                        tiOSmsTmp.set_referTime(m_dtDateTime);
                        tiOSmsTmp.set_referStaffId("CREDIT00");
                        tiOSmsTmp.set_referDepartId("CREDI");
                        tiOSmsTmp.set_dealTime(dealTime);
                        //tiOSmsTmp.set_dealStaffid(const aistring & value);
                        //tiOSmsTmp.set_dealDepartid(const aistring & value);
                        tiOSmsTmp.set_dealState("0");
                        //tiOSmsTmp.set_remark(const aistring & value);
                        //tiOSmsTmp.set_revc1(const aistring & value);
                        //tiOSmsTmp.set_revc2(const aistring & value);
                        //tiOSmsTmp.set_revc3(const aistring & value);
                        tiOSmsTmp.set_revc4(recv4); //结余值
                        tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));

                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms  data : eparchy_code= %s,queue_id=%lld,acct_id = %lld",
                                  tiOSmsTmp.get_eparchyCode().c_str(),
                                  m_imsNtfCredit.get_queueId(),
                                  m_imsNtfCredit.get_acctId());*/
                        
                        //add by xupp for yunnanV8 begin、
                        //int64 llSequenceId = 9999999999;
                        //llSequenceId = get_sequenceId("JD", "IMS_NTF_SEQ");
                        //llSequenceId = llSequenceId%100000000;
                        //AISTD string strQueueId = m_dtDateTime.toString("%M%D%H%N%S") + ltoa(llSequenceId);
                        tiOSmsTmp.set_tradeId( TI_O_SMS_SEQUECE_BASE + m_imsNtfCredit.get_queueId() );
                        //tiOSmsTmp.set_tradeId(strQueueId);
                        int16 iPartitionId = tiOSmsTmp.get_tradeId() % TI_O_SMS_PARTITION_MOD;
                        tiOSmsTmp.set_partitionId(iPartitionId);
                        
                        tiOSmsTmp.set_sourceCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
                        tiOSmsTmp.set_eparchyCode(eparchyCode);

                        AISTD string strSmsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                        tiOSmsTmp.set_inModeCode( strSmsKindCode.substr(0,1) );
                        tiOSmsTmp.set_sendObjectCode("01");
                        
                        tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                        tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                        tiOSmsTmp.set_receveObject(m_imsNtfCredit.get_phoneId());
                        tiOSmsTmp.set_sendObject("");
                        tiOSmsTmp.set_smsContent(m_imsNtfCredit.get_notifContent());
                        tiOSmsTmp.set_referTime(m_dtDateTime);
                        
                        tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                        tiOSmsTmp.set_referStaffId("CREDIT00");
                        tiOSmsTmp.set_referDepartId("CREDI");
                        
                        
                        tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));
                        tiOSmsTmp.set_day(atoi(m_dtDateTime.toString("%D").c_str()));

                        AISTD string strQueueIdT = ltoa(m_imsNtfCredit.get_queueId());
                        tiOSmsTmp.set_rsrvStr4(strQueueIdT);//SINGLE
                        
                        LOG_TRACE( "\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms  data : eparchy_code= %s,queue_id=%lld",
                                  tiOSmsTmp.get_eparchyCode().c_str(),
                                  tiOSmsTmp.get_tradeId() );
                        //add by xupp for yunnanV8 20210821 end

                        m_listTiOSms.push_back(tiOSmsTmp);
                        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit->insert_to_sms")
                        iRetValue = insertIntoSms(pSession, cErrorMsg);
                        ES_END_RUN_TIME
                    }
                    if((bSmsToTable==false))//配置了调用消息中心服务,需要调用消息中心
                    { // 调用服务
                        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit->call_crmService")
                        iRetValue = call_crmService(pSession, cErrorMsg);
                        ES_END_RUN_TIME
                    }
                    //    add by fsl@20191115 for 用户全生命周期信控短信提醒
                    try
                    {
                        for (CStringList::iterator itNotificationID = triggeringNotificationIDList.begin(); itNotificationID != triggeringNotificationIDList.end(); ++itNotificationID)
                        {

                            if (atoi((*itNotificationID).c_str()) == m_imsNtfCredit.get_notificationId())
                            {
                                // sysparameter表中配置存在该notification_id数据，做相应处理
                                trans2UserOweTrack(m_imsNtfCredit, m_cCaUserOweTrack);

                                if (ABMPROMPT_OK == queryUserOweTrackList(pSession, cErrorMsg))
                                {
                                    if (m_listCCaUserOweTrack.size() > 0)
                                    {
                                        if (cdk::strings::Atol64(m_cCaUserOweTrack.get_updateTime().toString("%Y%M%D%H%N%S")) > cdk::strings::Atol64(m_listCCaUserOweTrack[0].get_updateTime().toString("%Y%M%D%H%N%S")))
                                        {
                                            if (ABMPROMPT_OK != update_UserOweTrack(pSession, cErrorMsg))
                                            {
                                                TRACE_CA_USER_OWE_TRACE
                                            }
                                        }
                                    }
                                    else
                                    {
                                        m_listCCaUserOweTrack.clear();
                                        m_listCCaUserOweTrack.push_back(m_cCaUserOweTrack);
                                        if (ABMPROMPT_OK != insert_DataToUserOweTrack(pSession, cErrorMsg))
                                        {
                                            TRACE_CA_USER_OWE_TRACE
                                        }
                                    }
                                }
                                else
                                {
                                    TRACE_CA_USER_OWE_TRACE
                                }

                                break;
                            }
                        }
                    }
                    catch (...)
                    {
                        LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , CA_USER_OWE_TRACK catch unknow exception!!!========", __func__);
                    }
                }
            }
        }
        //else //瞬时超套客户外呼需求
        //{
        //    iRetValue = insertOneToBeyondPackage(pSession, cErrorMsg);
        //}

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit->delete_inserthis")
        if (iRetValue != ABMPROMPT_OK && iRetValue != ABMPROMPT_ORDER_PHONE)
        {
            //contrl_rollback(pSession, cErrorMsg);
            m_imsNtfCredit.set_status(ABM_PROMPT_PROCESS_FAILED);
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfCredit.set_remark(strRemark.substr(0, 1024));
            m_imsNtfCredit.set_soDate(m_dtDateTime);
            update_data<MAbmInterfacePromptDb::CImsNtfCreditList>(pSession, cErrorMsg, m_imsNtfCredit);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Credit sub table, %s : %s return to ims_ntf_credit",
                      m_imsNtfCredit.get_queueId(),
                      m_imsNtfCredit.get_acctId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfCreditHis imsNtfCreditHis;
            transToHis(m_imsNtfCredit, imsNtfCreditHis);
            if (iRetValue == ABMPROMPT_OK)
            {
                imsNtfCreditHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                if (bSmsToTable && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel && ENUM_ACTION_LEVEL_APP != m_nActionLevel)
                {
                    imsNtfCreditHis.set_remark("insert into ti_o_sms OK");
                }
                else if (bIsCallServiceFail && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel && ENUM_ACTION_LEVEL_APP != m_nActionLevel)
                {
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    strRemark.append("|insert into ti_o_sms OK");
                    imsNtfCreditHis.set_remark(strRemark.substr(0, 1024));
                }
                else if (ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel)
                {
                    imsNtfCreditHis.set_remark("insert into ti_o_fsbd_wxmsg OK");
                }
                else if (ENUM_ACTION_LEVEL_APP == m_nActionLevel)
                {
                    imsNtfCreditHis.set_remark("insert into ti_o_fsbd_appmsg OK");
                }
                else
                {
                    imsNtfCreditHis.set_remark((cErrorMsg.get_errorMsg() + ":" + cErrorMsg.get_hint()).substr(0, 2048));
                }
            }
            else
            {
                imsNtfCreditHis.set_status(ABM_PROMPT_PROCESS_PHONE_FILTER);
                imsNtfCreditHis.set_remark((cErrorMsg.get_errorCode() + ":" + cErrorMsg.get_hint()).substr(0, 2048));
            }

            imsNtfCreditHis.set_soDate(m_dtDateTime);
            imsNtfCreditHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
            m_listImsNtfCreditHis.push_back(imsNtfCreditHis);

            iRetValue = insert_dataToHisCredit(pSession, cErrorMsg);
            m_listImsNtfCreditHis.clear();

            if (ABMPROMPT_OK == iRetValue)
            {
                iRetValue = delete_data<MAbmInterfacePromptDb::CImsNtfCreditList>(cErrorMsg, m_imsNtfCredit);
                if (iRetValue != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    m_listStatUp.clear();
                    m_listStatIn.clear();
                    m_imsNtfCredit.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfCredit.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfCredit.set_soDate(m_dtDateTime);
                    update_data<MAbmInterfacePromptDb::CImsNtfCreditList>(pSession, cErrorMsg, m_imsNtfCredit);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Credit sub table, %s : %s return to ims_ntf_credit",
                              m_imsNtfCredit.get_queueId(),
                              m_imsNtfCredit.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_credit->statImsNtf")
                    statImsNtf(pSession, cErrorMsg, m_sysNotificationActionExtsmsIter->second.GetTradeTypeCode(), m_imsNtfCredit);
                    ES_END_RUN_TIME
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from credit sub_table is done successed",
                              m_imsNtfCredit.get_queueId(),
                              m_imsNtfCredit.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                }
            }
            else
            {
                contrl_rollback(pSession, cErrorMsg);
                m_listStatIn.clear();
                m_listStatUp.clear();
                m_imsNtfCredit.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfCredit.set_remark(strRemark.substr(0, 1024));
                m_imsNtfCredit.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfCreditList>(pSession, cErrorMsg, m_imsNtfCredit);
                contrl_commit(pSession, cErrorMsg);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Credit sub table, %s : %s return to ims_ntf_credit",
                          m_imsNtfCredit.get_queueId(),
                          m_imsNtfCredit.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
            }
        }

        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            upStatPrompt(pSession, cErrorMsg);
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME

        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    upStatPrompt(pSession, cErrorMsg);
    contrl_commit(pSession, cErrorMsg);

    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> loop process success : %d orders processed. ", m_listImsNtfCredit.size());
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_ivr(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_ivr->query_data")
    query_data(m_listImsNtfIvr);
    ES_END_RUN_TIME

    //没有捞到数据则休眠一段时间，休眠时间默认配置为5秒
    if (m_listImsNtfIvr.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfIvr is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_ivr order : %d orders from table %s.", m_listImsNtfIvr.size(), m_sourceTable.c_str());

    m_listImsNtfIvrHis.clear();
    int32 iCount = 0;

    for (m_itrImsNtfIvr = m_listImsNtfIvr.begin(); m_itrImsNtfIvr != m_listImsNtfIvr.end(); m_itrImsNtfIvr++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_ivr->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfIvr = *m_itrImsNtfIvr;
        m_actionId = m_imsNtfIvr.get_actionId();
        m_listImsNtfIvrHis.clear();
        int32 iRetValue = ABMPROMPT_OK;

        iRetValue = insertOneToBeyondPackage(pSession, cErrorMsg);

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_ivr->delete_inserthis")
        if (iRetValue != ABMPROMPT_OK && iRetValue != ABMPROMPT_ORDER_PHONE)
        {
            //contrl_rollback(pSession, cErrorMsg);
            m_imsNtfIvr.set_status(ABM_PROMPT_PROCESS_FAILED);
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfIvr.set_remark(strRemark.substr(0, 1024));
            m_imsNtfIvr.set_soDate(m_dtDateTime);
            update_data<MAbmInterfacePromptDb::CImsNtfIvrList>(pSession, cErrorMsg, m_imsNtfIvr);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Ivr sub table, %s : %s return to ims_ntf_ivr",
                      m_imsNtfIvr.get_queueId(),
                      m_imsNtfIvr.get_acctId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfIvrHis imsNtfIvrHis;
            transToHis(m_imsNtfIvr, imsNtfIvrHis);
            if (iRetValue == ABMPROMPT_OK)
            {
                imsNtfIvrHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                imsNtfIvrHis.set_remark((cErrorMsg.get_errorMsg() + ":" + cErrorMsg.get_hint()).substr(0, 2048));

            }
            else
            {
                imsNtfIvrHis.set_status(ABM_PROMPT_PROCESS_PHONE_FILTER);
                imsNtfIvrHis.set_remark((cErrorMsg.get_errorCode() + ":" + cErrorMsg.get_hint()).substr(0, 2048));
            }

            imsNtfIvrHis.set_soDate(m_dtDateTime);
            imsNtfIvrHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
            m_listImsNtfIvrHis.push_back(imsNtfIvrHis);

            iRetValue = insert_dataToHisIvr(pSession, cErrorMsg);
            m_listImsNtfIvrHis.clear();

            if (ABMPROMPT_OK == iRetValue)
            {
                iRetValue = delete_data<MAbmInterfacePromptDb::CImsNtfIvrList>(cErrorMsg, m_imsNtfIvr);
                if (iRetValue != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    //m_listStatUp.clear();
                    //m_listStatIn.clear();
                    m_imsNtfIvr.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfIvr.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfIvr.set_soDate(m_dtDateTime);
                    update_data<MAbmInterfacePromptDb::CImsNtfIvrList>(pSession, cErrorMsg, m_imsNtfIvr);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Ivr sub table, %s : %s return to ims_ntf_ivr",
                              m_imsNtfIvr.get_queueId(),
                              m_imsNtfIvr.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    // ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_ivr->statImsNtf")
                    //statImsNtf(pSession, cErrorMsg, m_sysNotificationActionExtsmsIter->second.GetTradeTypeCode(), m_imsNtfCredit);
                    //ES_END_RUN_TIME
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from ivr sub_table is done successed",
                              m_imsNtfIvr.get_queueId(),
                              m_imsNtfIvr.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                }
            }
            else
            {
                contrl_rollback(pSession, cErrorMsg);
                //m_listStatIn.clear();
                //m_listStatUp.clear();
                m_imsNtfIvr.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfIvr.set_remark(strRemark.substr(0, 1024));
                m_imsNtfIvr.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfIvrList>(pSession, cErrorMsg, m_imsNtfIvr);
                contrl_commit(pSession, cErrorMsg);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Ivr sub table, %s : %s return to ims_ntf_ivr",
                          m_imsNtfIvr.get_queueId(),
                          m_imsNtfIvr.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
            }
        }

        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            //upStatPrompt(pSession, cErrorMsg);
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME

        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    //upStatPrompt(pSession, cErrorMsg);
    contrl_commit(pSession, cErrorMsg);

    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> loop process success : %d orders processed. ", m_listImsNtfIvr.size());
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_sts(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC

    static int stopCount = 0;
    bool isNotOpenProcess = false;
    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->query_data")
    query_data(m_listImsNtfSts);
    ES_END_RUN_TIME

    if (m_pCfg->m_cfgParams.m_stsType == STS_TYPE_OPEN && m_pCfg->m_cfgParams.m_bomcLogSwitch)
    {
        //批量写BOMC文件，内部校验提交条件，在此处执行为了没有工单处理的时候，之前的数据能提交
        if (!m_bomcFile.writeFile())
        {
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> write bomc file fail!");
        }
    }
    else
    {
        isNotOpenProcess = true;
    }

    if (m_listImsNtfSts.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        if (m_isNeedUpdateStatus)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfStsList, MAbmInterfacePromptDb::CImsNtfSts>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfSts is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_sts order : %d orders from table %s.", m_listImsNtfSts.size(), m_sourceTable.c_str());

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->repeat_filter")
    repeat_filter(m_listImsNtfSts.begin(), m_listImsNtfSts.end(), m_userList);
    ES_END_RUN_TIME
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_sts order user count =  %d  from table %s.", m_userList.size(), m_sourceTable.c_str());

    m_listImsNtfSts.clear();
    int32 iCount = 0;
    m_listStatIn.clear();
    m_listStatUp.clear();
    //控制是否校验结余开关 add by taocj@20181229
    aistring strval;
    strval.clear();
    m_filterstop = false;
    get_sysParamter("FILTER_STOP_BY_BALANCE", strval);
    //0:停机工单不判余额 1:停机工单判断余额
    if (strval == "1")
    {
        m_filterstop = true;
    }
    //end mod

    for (m_itrUserList = m_userList.begin(); m_itrUserList != m_userList.end(); m_itrUserList++)
    {
        //get_time;
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        int32 retCode = get_dataSts(pSession, cErrorMsg);

        //停开机错单表初始化
        m_listImsNtfStsErrHis.clear();
        m_listImsNtfStsHis.clear();
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> serv_id = %lld 's  m_listImsNtfSts.size() = %d ...", *m_itrUserList, m_listImsNtfSts.size());
        if (m_listImsNtfSts.empty())
        {
            continue;
        }
        m_imsNtfSts = m_listImsNtfSts[0];
        iCount += m_listImsNtfSts.size();
        bool isProc = true;
        bool isNeedDeal = true; // 根据结余对停机工单判断

        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> start judge newer, serv_id = %lld ...", *m_itrUserList);

        //当前进程是开机进程。取第一条最新的工单，若最新工单不是开机类工单，直接跳过，让停机进程处理。
        if (m_pCfg->m_cfgParams.m_stsType == STS_TYPE_OPEN)
        {
            if (m_imsNtfSts.get_stateId() != m_pCfg->m_cfgCommon.m_openStateId)
            {
                //Todo:需要处理前面开的工单：开-》停=停
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> serv_id = %lld 's orders size  = %d 's last state_id = %d, but the proc sts_type = %d.",
                          *m_itrUserList, m_listImsNtfSts.size(),
                          m_imsNtfSts.get_stateId(),
                          m_pCfg->m_cfgParams.m_stsType);
                isProc = false;
                //continue;
            }

			if (isProc && m_pCfg->m_cfgParams.m_bomcLogSwitch)
            {

                m_bomcFile.setParentId("");
                m_bomcFile.setTraceId("");
                m_bomcFile.setFrontTime(0, 0);

                //BOMC设置开始信息
                m_bomcFile.resetTime();
                if (m_bomcFile.hasTraceId())
                {
                    m_bomcFile.regStartTime();
                    m_bomcFile.generateUuid(); //生成当前业务序列
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> get bomc trace: trace_id = %s, parent_id = %s, uuid = %s",
                              m_bomcFile.getTraceId(), m_bomcFile.getParentId(), m_bomcFile.getUuid());
                }
                
				//BOMC设置主机IP地址
				string sLoginIp = "";
				if(ABMPROMPT_OK == getLocalIp(sLoginIp))
				{
					LOG_TRACE("sLoginIp:%s",sLoginIp.c_str());
					m_bomcFile.setIp(sLoginIp.c_str());
				}				
				LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> get busIp = %s",m_bomcFile.getIp());

                m_bomcFile.setAcctId((long)m_imsNtfSts.get_acctId());
                m_bomcFile.setUserId((long)m_imsNtfSts.get_resourceId());
				m_bomcFile.setPhoneId(m_imsNtfSts.get_phoneId().c_str());
            }
        }
        else //当前进程是停机进程。取第一条最新的工单，若最新工单是开机类工单，直接跳过，让开机进程处理。
        {
            if (m_imsNtfSts.get_stateId() == m_pCfg->m_cfgCommon.m_openStateId)
            {
                //Todo:需要处理前面停的工单：停-》开=开
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> serv_id = %lld 's orders size  = %d 's last state_id = %d, but the proc sts_type = %d.",
                          *m_itrUserList, m_listImsNtfSts.size(),
                          m_imsNtfSts.get_stateId(),
                          m_pCfg->m_cfgParams.m_stsType);
                isProc = false;
                //continue;
            }
            else if (isNeedDeal == true)
            {				
                if (!m_filterstop)
                {
                }
                else if(m_imsNtfSts.get_notificationType()==60)//湖南小区停机不要判断余额
                {

                }
                else if (!check_leaveFee(m_imsNtfSts.get_acctId(), m_imsNtfSts.get_notificationId(), pSession)) // 如果结余大于0，则不停机
                {
                    isNeedDeal = false;
                }
            }
        }

        //如果不是当前处理工单，直接把超时工单移到历史表中
        if (!isProc)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->delete_inserthis_update")
            for (m_itrImsNtfSts = m_listImsNtfSts.begin(); m_itrImsNtfSts != m_listImsNtfSts.end(); m_itrImsNtfSts++)
            {
                int64 llQueueId = 0;
                m_listImsNtfStsHis.clear();
                MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                transToHis((*m_itrImsNtfSts), imsNtfStsHis);

                if (m_itrImsNtfSts == m_listImsNtfSts.begin())
                {
                    llQueueId = m_itrImsNtfSts->get_queueId();
                    continue;
                }
                else
                {
                    imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                    if (llQueueId > 0)
                    {
                        imsNtfStsHis.set_remark("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                    }
                    else
                    {
                        imsNtfStsHis.set_remark("Out time orders, remove to his. ");
                    }
                }
                imsNtfStsHis.set_soDate(m_dtDateTime);
                imsNtfStsHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                m_listImsNtfStsHis.push_back(imsNtfStsHis);

                if (imsNtfStsHis.get_queueId() != llQueueId)
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld refer to queue_id=%lld is time out and removed to his.", imsNtfStsHis.get_queueId(), llQueueId);
                }
                stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
            }
            ES_END_RUN_TIME
        }
        else
        {
            //调用接口，根据接口返回码做不同的处理
            aistring strCallCrmTime("");
            if (isNeedDeal)
            {
                m_actionId = m_imsNtfSts.get_actionId();
                //获取接口地址等信息
                if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
                {
                    AISTD string strErrMsg("action_id = ");
                    strErrMsg.append(cdk::strings::Itoa(m_actionId));
                    strErrMsg.append(" get action_id info failed!");
                    cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
                    cErrorMsg.set_hint(strErrMsg);
                    retCode = ABMPROMPT_ERROR;
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
                }

                if (retCode == ABMPROMPT_OK)
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> call crm to change user's stop/open status data : eparchy_code= %d,queue_id=%lld,acct_id = %lld",
                              m_imsNtfSts.get_regionCode(),
                              m_imsNtfSts.get_queueId(),
                              m_imsNtfSts.get_acctId());
                    m_dtDateTime = CBSDateTime::currentDateTime();
                    strCallCrmTime.append("|");
                    strCallCrmTime.append(m_dtDateTime.toString("%Y%M%D%H%N%S"));

                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->call_crmService")
                    retCode = call_crmService(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                    strCallCrmTime.append(":");
                    m_dtDateTime = CBSDateTime::currentDateTime();
                    strCallCrmTime.append(m_dtDateTime.toString("%Y%M%D%H%N%S"));
                }

                //端到端文件状态编码 add by qianwj@202002 for hunan
                if (ABMPROMPT_OK == retCode && m_pCfg->m_cfgParams.m_bomcLogSwitch)
                {
                    m_bomcFile.setBizStatus(0);
                }
                else
                {
                    m_bomcFile.setBizStatus(1);
                }

                bool dmlTag = true;
                m_dtDateTime = CBSDateTime::currentDateTime();

                //在前面数据有网络数据，且本次操作网络已经连通
                if (m_isNeedUpdateStatus &&
                    cErrorMsg.get_errorMsg() != "-100" &&
                    cErrorMsg.get_errorMsg() != "-101" &&
                    cErrorMsg.get_errorMsg() != "-102" &&
                    cErrorMsg.get_errorMsg() != "-103")
                {
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                    update_status<MAbmInterfacePromptDb::CImsNtfStsList, MAbmInterfacePromptDb::CImsNtfSts>(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                }
            }

            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->delete_inserthis_update")
            if (ABMPROMPT_OK == retCode)
            {
                //生成历史表工单
                m_listImsNtfStsHis.clear();
                int64 llQueueId = 0;
                for (m_itrImsNtfSts = m_listImsNtfSts.begin(); m_itrImsNtfSts != m_listImsNtfSts.end(); m_itrImsNtfSts++)
                {
                    m_listImsNtfStsHis.clear();
                    m_listImsNtfStsErrHis.clear();

                    MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                    MAbmInterfacePromptDb::CImsNtfStserrHis imsNtfStsErrHis;

                    if (m_itrImsNtfSts == m_listImsNtfSts.begin())
                    {
                        transToHis((*m_itrImsNtfSts), imsNtfStsHis);
                        llQueueId = m_itrImsNtfSts->get_queueId();
                        AISTD string strRemark;
                        if (!isNeedDeal)
                        {
                            // 失效的停机工单
                            imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_INVALID);
                            strRemark = "leavefee more than 0, Invalid!";
                        }
                        else if (ABMPROMPT_REPEATED == retCode)
                        {
                            imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_STS_REPEAT);
                            strRemark = cErrorMsg.get_errorMsg();
                            strRemark.append(":");
                            strRemark.append(cErrorMsg.get_hint());
                            strRemark.append(strCallCrmTime);
                        }
                        else if (ABMPROMPT_OK == retCode)
                        {
                            imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_sts->statImsNtf")
                            statImsNtf(pSession, cErrorMsg, m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(), *m_itrImsNtfSts);
                            ES_END_RUN_TIME
                            strRemark = cErrorMsg.get_errorMsg();
                            strRemark.append(":");
                            strRemark.append(cErrorMsg.get_hint());
                            strRemark.append(strCallCrmTime);
                        }
                        imsNtfStsHis.set_remark(strRemark.substr(0, 1024));

                        imsNtfStsHis.set_soDate(m_dtDateTime);
                        imsNtfStsHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                        m_listImsNtfStsHis.push_back(imsNtfStsHis);
                    }
                    else
                    {
                        transToHis((*m_itrImsNtfSts), imsNtfStsHis);
                        imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                        if (llQueueId > 0)
                        {
                            imsNtfStsHis.set_remark("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                        }
                        else
                        {
                            imsNtfStsHis.set_remark("Out time orders, remove to his. ");
                        }
                        imsNtfStsHis.set_soDate(m_dtDateTime);
                        m_listImsNtfStsHis.push_back(imsNtfStsHis);
                    }

                    if (imsNtfStsHis.get_queueId() != llQueueId)
                    {
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.", imsNtfStsHis.get_queueId(), llQueueId);
                    }
                    stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                }
            }
            else if (ABMPROMPT_REPEATED == retCode)
            {
                int64 llQueueId = 0;
                for (m_itrImsNtfSts = m_listImsNtfSts.begin(); m_itrImsNtfSts != m_listImsNtfSts.end(); ++m_itrImsNtfSts)
                {
                    //生成错单表工单
                    m_listImsNtfStsErrHis.clear();
                    m_listImsNtfStsHis.clear();
                    MAbmInterfacePromptDb::CImsNtfStserrHis imsNtfStsErrHis;
                    MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                    transToHis((*m_itrImsNtfSts), imsNtfStsHis);

                    if (m_itrImsNtfSts == m_listImsNtfSts.begin())
                    {
                        llQueueId = m_itrImsNtfSts->get_queueId();
                        imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_STS_REPEAT);
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfStsHis.set_notifContent(strRemark.substr(0, 1024));
                    }
                    else
                    {
                        imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                        if (llQueueId > 0)
                        {
                            imsNtfStsHis.set_notifContent("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                        }
                        else
                        {
                            imsNtfStsHis.set_notifContent("Out time orders, remove to his. ");
                        }
                    }
                    imsNtfStsHis.set_soDate(m_dtDateTime);
                    m_listImsNtfStsHis.push_back(imsNtfStsHis);

                    if (imsNtfStsHis.get_queueId() != llQueueId)
                    {
                    	LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.", imsNtfStsHis.get_queueId(), llQueueId);
                    }
                    stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                }
            }
            else if (ABMPROMPT_ERROR == retCode)
            {
                int64 llQueueId = 0;
                for (m_itrImsNtfSts = m_listImsNtfSts.begin(); m_itrImsNtfSts != m_listImsNtfSts.end(); ++m_itrImsNtfSts)
                {
                    m_listImsNtfStsErrHis.clear();
                    m_listImsNtfStsHis.clear();
                    if (m_itrImsNtfSts == m_listImsNtfSts.begin())
                    {
                        MAbmInterfacePromptDb::CImsNtfSts imsNtfSts = *m_itrImsNtfSts;
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfSts.set_status(ABM_PROMPT_PROCESS_FAILED); //处理错误，需要手工处理
                        llQueueId = m_itrImsNtfSts->get_queueId();
                        //需重置的错单有：
                        //-100 字符串解析错，
                        //-101 调CRM 服务错，
                        //-102 调用HTTP服务错
                        //-103 无返回码
                        //CRM_COMM_983 用户正在办理其它业务中
                        if (cErrorMsg.get_errorMsg() == "-100" ||
                            cErrorMsg.get_errorMsg() == "-101" ||
                            cErrorMsg.get_errorMsg() == "-102" ||
                            cErrorMsg.get_errorMsg() == "-103" ||
                            cErrorMsg.get_errorMsg() == "CRM_COMM_983")
                        {
                            if (imsNtfSts.get_stateDtlId() < STS_START_STATE + m_iUpdateCrmExcep)
                            {
                                imsNtfSts.set_status(atoi(cErrorMsg.get_errorMsg().c_str()));
                                imsNtfSts.set_stateDtlId(imsNtfSts.get_stateDtlId() + 1);
                                m_isNeedUpdateStatus = true;
                                imsNtfSts.set_remark(strRemark.substr(0, 1024));
                                imsNtfSts.set_soDate(m_dtDateTime);
                                update_data<MAbmInterfacePromptDb::CImsNtfStsList>(pSession, cErrorMsg, imsNtfSts);
                            }
                            else
                            {
                                //恢复值state_dtl_id的值
                                imsNtfSts.set_stateDtlId(STS_START_STATE);
                                imsNtfSts.set_status(ABM_PROMPT_PROCESS_PROC_TRY);
                                //strRemark.append(":网络问题,重做失败!");
                                MAbmInterfacePromptDb::CImsNtfStserrHis imsNtfStsErrHis;
                                transToHis((*m_itrImsNtfSts), imsNtfStsErrHis);
                                imsNtfStsErrHis.set_status(ABM_PROMPT_PROCESS_PROC_TRY);
                                imsNtfStsErrHis.set_remark(strRemark.substr(0, 1024));
                                imsNtfStsErrHis.set_soDate(m_dtDateTime);
                                imsNtfStsErrHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                                m_listImsNtfStsErrHis.push_back(imsNtfStsErrHis);
                                stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                            }
                        }
                        else
                        {
                            MAbmInterfacePromptDb::CImsNtfStserrHis imsNtfStsErrHis;
                            transToHis((*m_itrImsNtfSts), imsNtfStsErrHis);
                            imsNtfStsErrHis.set_status(ABM_PROMPT_PROCESS_FAILED);
                            if (CRM_RETURN_CODE_CRM_BOF_002 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfStsErrHis.set_errStatus(AMB_CRM_CRM_BOF_002);
                            }
                            else if (CRM_RETURN_CODE_CRM_CUST_35 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfStsErrHis.set_errStatus(ABM_CRM_CRM_CUST_35);
                            }
                            else if (CRM_RETURN_CODE_CRM_USER_112 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfStsErrHis.set_errStatus(ABM_CRM_CRM_USER_112);
                            }
                            else if (CRM_RETURN_CODE_888888 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfStsErrHis.set_errStatus(ABM_CRM_888888);
                            }
                            else if (CRM_RETURN_CODE_201711 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfStsErrHis.set_errStatus(ABM_CRM_201711);
                            }
                            imsNtfStsErrHis.set_remark(strRemark.substr(0, 1024));
                            imsNtfStsErrHis.set_soDate(m_dtDateTime);
                            imsNtfStsErrHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                            m_listImsNtfStsErrHis.push_back(imsNtfStsErrHis);
                            stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                        }
                        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld ,acct_id = %lld come from sts sub_table, %s ：%s ,return ims_ntf_sts",
                                  imsNtfSts.get_queueId(),
                                  imsNtfSts.get_acctId(),
                                  cErrorMsg.get_errorMsg().c_str(),
                                  cErrorMsg.get_hint().c_str());
                        continue;
                    }

                    MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                    transToHis((*m_itrImsNtfSts), imsNtfStsHis);
                    imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                    if (llQueueId > 0)
                    {
                        imsNtfStsHis.set_remark("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                    }
                    else
                    {
                        imsNtfStsHis.set_remark("Out time orders, remove to his. ");
                    }
                    imsNtfStsHis.set_soDate(m_dtDateTime);
                    imsNtfStsHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.",
                              imsNtfStsHis.get_queueId(),
                              llQueueId);
                    m_listImsNtfStsHis.push_back(imsNtfStsHis);
                    stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                }
            }
            else if (ABMPROMPT_SLOWSPEED == retCode) //CRM侧返回需要降速，CRM错误码：CRM_TRADECREDIT_999
            {
                m_listImsNtfStsErrHis.clear();
                m_listImsNtfStsHis.clear();
                int64 llQueueId = 0;
                for (m_itrImsNtfSts = m_listImsNtfSts.begin(); m_itrImsNtfSts != m_listImsNtfSts.end(); ++m_itrImsNtfSts)
                {
                    m_listImsNtfStsErrHis.clear();
                    m_listImsNtfStsHis.clear();
                    //m_dtDateTime = CBSDateTime::currentDateTime();
                    if (m_listImsNtfSts.begin() == m_itrImsNtfSts && m_itrImsNtfSts->get_stateDtlId() < STS_START_STATE + m_iUpdateCrmExcep)
                    {
                        llQueueId = m_itrImsNtfSts->get_queueId();
                        MAbmInterfacePromptDb::CImsNtfSts imsNtfSts = *m_itrImsNtfSts;
                        imsNtfSts.set_status(ABM_PROMPT_PROCESS_UNDO); //直接下次处理
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfSts.set_remark(strRemark.substr(0, 1024));
                        imsNtfSts.set_stateDtlId(imsNtfSts.get_stateDtlId() + 1);
                        imsNtfSts.set_soDate(m_dtDateTime.addSecs(60 * 5 * (imsNtfSts.get_stateDtlId() - STS_START_STATE)));
                        LOG_TRACE(imsNtfSts.to_string().c_str());
                        update_data<MAbmInterfacePromptDb::CImsNtfStsList>(pSession, cErrorMsg, imsNtfSts);
                        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld ,acct_id = %lld come from sts sub_table, %s ：%s ,return ims_ntf_sts",
                                  imsNtfSts.get_queueId(),
                                  imsNtfSts.get_acctId(),
                                  cErrorMsg.get_errorMsg().c_str(),
                                  cErrorMsg.get_hint().c_str());
                        continue;
                    }

                    MAbmInterfacePromptDb::CImsNtfStserrHis imsNtfStsErrHis;
                    MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                    transToHis((*m_itrImsNtfSts), imsNtfStsErrHis);
                    imsNtfStsErrHis.set_soDate(m_dtDateTime);
                    if (m_listImsNtfSts.begin() == m_itrImsNtfSts)
                    {
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        llQueueId = m_itrImsNtfSts->get_queueId();
                        imsNtfStsErrHis.set_remark(strRemark.substr(0, 1024));
                        imsNtfStsErrHis.set_status(ABM_PROMPT_PROCESS_PROC_TRY);
                        imsNtfStsErrHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                    	m_listImsNtfStsErrHis.push_back(imsNtfStsErrHis);
                    }
                    else if (llQueueId > 0)
                    {
                    	transToHis((*m_itrImsNtfSts), imsNtfStsHis);
                        imsNtfStsHis.set_soDate(m_dtDateTime);
                        imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                        imsNtfStsHis.set_remark("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and remove to his.", imsNtfStsHis.get_queueId(), llQueueId);
                        imsNtfStsHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                    	m_listImsNtfStsHis.push_back(imsNtfStsHis);
                    }
                    
                    stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                }
            }
            else // 其他错误
            {
                //生成历史表工单
                m_listImsNtfStsErrHis.clear();
                m_listImsNtfStsHis.clear();
                int64 llQueueId = 0;
                for (m_itrImsNtfSts = m_listImsNtfSts.begin(); m_itrImsNtfSts != m_listImsNtfSts.end(); m_itrImsNtfSts++)
                {
                    m_listImsNtfStsErrHis.clear();
                    m_listImsNtfStsHis.clear();
                    if (m_itrImsNtfSts == m_listImsNtfSts.begin())
                    {
                        llQueueId = m_itrImsNtfSts->get_queueId();
                        MAbmInterfacePromptDb::CImsNtfSts imsNtfSts = *m_itrImsNtfSts;
                        imsNtfSts.set_status(ABM_PROMPT_PROCESS_RETRY); //2也表示错单，但是要重试的
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfSts.set_remark(strRemark.substr(0, 1024));
                        imsNtfSts.set_soDate(m_dtDateTime);
                        update_data<MAbmInterfacePromptDb::CImsNtfStsList>(pSession, cErrorMsg, imsNtfSts);
                        //统一提交
                        //contrl_commit(pSession,cErrorMsg);
                        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld ,acct_id = %lld call crm is failed ,come from sts sub_table, %s ：%s ,return ims_ntf_sts",
                                  imsNtfSts.get_queueId(),
                                  imsNtfSts.get_acctId(),
                                  cErrorMsg.get_errorMsg().c_str(),
                                  cErrorMsg.get_hint().c_str());
                        continue;
                    }
                    MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                    transToHis((*m_itrImsNtfSts), imsNtfStsHis);
                    imsNtfStsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                    if (llQueueId > 0)
                    {
                        imsNtfStsHis.set_remark("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                    }
                    else
                    {
                        imsNtfStsHis.set_remark("Out time orders, remove to his. ");
                    }
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld  is out time orders, refer to queue_id=%lld remove to his.", imsNtfStsHis.get_queueId(), llQueueId);

                    imsNtfStsHis.set_soDate(m_dtDateTime);
                    imsNtfStsHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
                    m_listImsNtfStsHis.push_back(imsNtfStsHis);
                    stsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfSts);
                }
            }

            if (m_pCfg->m_cfgParams.m_stsType == STS_TYPE_OPEN && m_pCfg->m_cfgParams.m_bomcLogSwitch)
            {
                //BOMC记录结束时间
                m_bomcFile.regEndTime();

                //BOMC错误信息
                AISTD string strBomcRemark(cErrorMsg.get_errorMsg());
                strBomcRemark.append(":");
                strBomcRemark.append(cErrorMsg.get_hint());
                strBomcRemark.append(":");
                m_dtDateTime = CBSDateTime::currentDateTime();
                strBomcRemark.append(m_dtDateTime.toString("%Y%M%D%H%N%S"));

                //逐条记录结果
                if (m_pCfg->m_cfgParams.m_bomcLogLevel == 0) //记录全部日志
                {
                    m_bomcFile.recordResult((int)m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(),
                                            strBomcRemark.substr(0, 256).c_str());
                }
                else if (m_pCfg->m_cfgParams.m_bomcLogLevel == 1 && m_bomcFile.getBizStatus() == 1) //只记录错误日志
                {
                    m_bomcFile.recordResult((int)m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(),
                                            strBomcRemark.substr(0, 256).c_str());
                }
            }
            ES_END_RUN_TIME
        }

        if (iCount >= m_pCfg->m_cfgCommon.m_iCommitNum)
        {
            upStatPrompt(pSession, cErrorMsg);
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }

        if (1 == retCode && m_pCfg->m_cfgCommon.m_nSleep > 0)
        {
            //由于速度太快，CRM处理不过来，需要等待几秒
            sleep(m_pCfg->m_cfgCommon.m_nSleep);
        }
        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    upStatPrompt(pSession, cErrorMsg);
    contrl_commit(pSession, cErrorMsg);

    /* 
        通过统计半停、停机、预销号、销号工单执行数，限制非开机工单的调用CRM速度，以达到不月初忙时不影响CRM侧开机工单正常完成。
        Added By xuxl3@asiainfo.com
    */
    if (true == isNotOpenProcess && thresholdForSleep > 0)
    {
        stopCount = m_listImsNtfSts.size() + stopCount;
        if (stopCount >= thresholdForSleep)
        {
            //达到休眠的阀值。
            sleep(m_pCfg->m_cfgCommon.m_nSleep);
            stopCount = 0;
        }
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_remind(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->query_data")
    query_data(m_listImsNtfRemind);
    ES_END_RUN_TIME
    if (m_listImsNtfRemind.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfRemind is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_remind order : %d orders from table %s.", m_listImsNtfRemind.size(), m_sourceTable.c_str());

    int32 iCount = 0;
    for (m_itrImsNtfRemind = m_listImsNtfRemind.begin(); m_itrImsNtfRemind != m_listImsNtfRemind.end(); ++m_itrImsNtfRemind)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfRemind = *m_itrImsNtfRemind;
        m_actionId = m_itrImsNtfRemind->get_actionId();
        m_listImsNtfRemindHis.clear();

        int32 retCode = ABMPROMPT_OK;
        bool bSmsToTable = true;
        bool bIsCallServiceFail = false;
        m_isSmsMarket = false;

        if (m_imsNtfRemind.get_notifContent().empty())
        {
            retCode = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
            cErrorMsg.set_hint("notif_content is empty");
        }
        else if (!check_phoneId(m_imsNtfRemind.get_phoneId()))
        {
            retCode = ABMPROMPT_ORDER_PHONE;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ORDER_PHONE));
            cErrorMsg.set_hint("phone_id is inaccurate,don't need to send message");
        }
        else if (!check_notifyContent(m_imsNtfRemind.get_notifContent()))
        {
            retCode = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ERROR_MESSAGE));
            cErrorMsg.set_hint("the notif_content contains  '<' or '>' ,some param_id don't have be replaced");
        }
        else
        {
            AISTD string strExtContent = "";
            int64 llTemplateId = m_imsNtfRemind.get_templateId();
            int64 llResourceId = m_imsNtfRemind.get_resourceId();
            int32 iRegionCode = m_imsNtfRemind.get_regionCode();
            AISTD string strTemplateFlag("");
            if (llTemplateId > 0)
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->getExtContent")
                int32 iRetV = getExtContent(iRegionCode, llResourceId, llTemplateId, strExtContent, strTemplateFlag);
                ES_END_RUN_TIME
            }
            //如果查询结果为空需要替换掉REMIND_REPLACE
            //根据template_flag字段区分，普通为0，需要替换的形式为[URT:1004]
            if (strTemplateFlag.substr(0, strlen(REMIND_TEMPLATE)) == REMIND_TEMPLATE)
            //if(strTemplateFlag.size()>1)
            {
                AISTD string strContent = m_imsNtfRemind.get_notifContent();
                //strContent += strExtContent;
                AISTD string strContRes(""); //短信替换结果
                replaceRemindContent(strContent, REMIND_REPLACE, strExtContent, strContRes);
                if (!strContRes.empty())
                {
                    m_imsNtfRemind.set_notifContent(strContRes);
                }
                else
                {
                    retCode = ABMPROMPT_ERROR;
                    cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
                    cErrorMsg.set_hint("notif_content is empty");
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> the content is empty,please check.");
                }
            }
        }

        // 从CRM 获取达量优速速率，替换短信模板中REMIND_LIMIT_SPEED
        AISTD string strContent = m_imsNtfRemind.get_notifContent();
        size_t bpos = strContent.find(REMIND_LIMIT_SPEED);
        if (bpos != std::string::npos)
        {
            AISTD string t_limitSpeed = "";
            if (ABMPROMPT_OK == get_limitSpeed(pSession, cErrorMsg, t_limitSpeed))
            {
                AISTD string strContent = m_imsNtfRemind.get_notifContent();
                AISTD string strContRes(""); //短信替换结果
                replaceRemindContent(strContent, REMIND_LIMIT_SPEED, t_limitSpeed, strContRes);
                if (!strContRes.empty())
                {
                    m_imsNtfRemind.set_notifContent(strContRes);
                }
                else
                {
                    retCode = ABMPROMPT_ERROR;
                    cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
                    cErrorMsg.set_hint("notif_content is empty");
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> the content is empty,please check.");
                }
            }
            else
            {
                retCode = ABMPROMPT_ERROR;
                //cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1)); //modified by sunjz 20210331
                //cErrorMsg.set_hint("get limit speed error ");  //modified by sunjz 20210331
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> get limit speed error,please check CRM %s interface", m_crmHttpQueryLimitSpeedName.c_str());
            }
        }
        LOG_TRACE("[ABM_SERV_ABMPROMPT]after speed=======>m_crmHttpServiceName:%s", m_crmHttpServiceName.c_str());
	

        if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
        {
            AISTD string strErrMsg("action_id = ");
            strErrMsg.append(cdk::strings::Itoa(m_actionId));
            strErrMsg.append(" get action_id info failed!");
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
            cErrorMsg.set_hint(strErrMsg);
            retCode = ABMPROMPT_ERROR;
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
        }

        LOG_TRACE("[ABM_SERV_ABMPROMPT]after get_actionExtInfo=======>m_crmHttpServiceName:%s", m_crmHttpServiceName.c_str());
        //记录夹带短信内容，插入工单历史表，共经分统计短信夹带情况
        AISTD string strExternSms = "";                        //放夹带的短信
        PROCESS_STATUS hisStatus = ABM_PROMPT_PROCESS_SUCCESS; //提醒短信历史表状态

        if (ABMPROMPT_OK == retCode)
        {
            m_nActionLevel = m_imsNtfRemind.get_actionLevel();
            if (ENUM_ACTION_LEVEL_APP == m_nActionLevel) //app提醒工单
            {
                //准备数据
                m_listTiOFsbdAppmsg.clear();

                AISTD string strQueueId = ltoa(m_imsNtfRemind.get_queueId());

                //注意：当工单量每秒超过百万条时，这种batchId拼接方式会出现重复
                AISTD string strAppBatchId = "ACT" + m_dtDateTime.toString("%Y%M%D%H%N%S") + strQueueId.substr(strQueueId.length() - 6); //账务为ACT+YYYYMMDDHHmmSS+6位序列号
                int32 iPartitionId = atoi(strAppBatchId.substr(strAppBatchId.length() - 3).c_str());

                AISTD string strTemplateContent = "";
                AISTD string strTitle = "";
                AISTD string strAddressType = "";
                int32 iAddressType = 0;
                AISTD string strAddress = "";
                AISTD string strMsgType = "";
                int32 iMonth = 0;
                int32 iDay = 0;
                int64 llResourceId = m_imsNtfRemind.get_resourceId();
                char eparchyCode[5];
                sprintf(eparchyCode, "%04d", m_imsNtfRemind.get_regionCode());
                AISTD string strRecvObject = m_imsNtfRemind.get_phoneId();

                iMonth = atoi(m_dtDateTime.toString("%M").c_str());
                iDay = atoi(m_dtDateTime.toString("%D").c_str());

                //提醒相关信息由于字段不足拼接在同一个字段中
                LOG_TRACE("[WXMSG]=======>get_notifContent:%s", m_imsNtfRemind.get_notifContent().c_str());
                CStringList lstStrWithMsgType;
                CStringList lstStr;
                int32 iMsgTypeSplitRet = split_msgType_notifyContent(m_imsNtfRemind.get_notifContent(), "|||", lstStrWithMsgType);
                if (iMsgTypeSplitRet == 2)
                {
                    strMsgType = lstStrWithMsgType.at(1);
                    base_divide_str(lstStrWithMsgType.at(0), "|", lstStr);
                    if (lstStr.size() == 3)
                    {
                        strTitle = lstStr.at(0);
                        strTemplateContent = lstStr.at(1);
                        strAddressType = lstStr.at(2);
                    }

                    if (lstStr.size() == 4)
                    {
                        strTitle = lstStr.at(0);
                        strTemplateContent = lstStr.at(1);
                        strAddressType = lstStr.at(2);
                        strAddress = lstStr.at(3);
                    }
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_imsNtfCredit.get_notifContent error");
                }
                LOG_TRACE("[WXMSG]=======>strMsgType:%s", strMsgType.c_str());

                iAddressType = strAddressType == "" ? 0 : atoi(strAddressType.c_str());

                MAbmInterfacePromptDb::CTiOFsbdAppmsg tiOFsbdAppMsg;
                tiOFsbdAppMsg.set_appBatchId(strAppBatchId);
                tiOFsbdAppMsg.set_partitionId(iPartitionId);
                tiOFsbdAppMsg.set_recvId(llResourceId);
                tiOFsbdAppMsg.set_recvObject(strRecvObject);
                tiOFsbdAppMsg.set_messageTitle(strTitle);
                tiOFsbdAppMsg.set_messageDescription(strTemplateContent);
                tiOFsbdAppMsg.set_addressType(iAddressType);
                tiOFsbdAppMsg.set_address(strAddress);
                tiOFsbdAppMsg.set_isLogin(0);   //是否强制登录，1-是，0-否
                tiOFsbdAppMsg.set_msgType(strMsgType); //消息类型，3：提醒类
                tiOFsbdAppMsg.set_sendType(0);  //立即发送
                tiOFsbdAppMsg.set_pushType(0);  //单播
                tiOFsbdAppMsg.set_pushLevel(1); //高级
                tiOFsbdAppMsg.set_endpointId("cmcc1");
                tiOFsbdAppMsg.set_eparchyCode(eparchyCode);
                tiOFsbdAppMsg.set_referTime(m_dtDateTime);
                tiOFsbdAppMsg.set_dealTime(m_dtDateTime);
                tiOFsbdAppMsg.set_dealState(0);
                tiOFsbdAppMsg.set_month(iMonth);
                tiOFsbdAppMsg.set_day(iDay);

                m_listTiOFsbdAppmsg.push_back(tiOFsbdAppMsg);
                //入表
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertTiOFsbdAppmsg")
                retCode = insertTiOFsbdAppmsg(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            else if (ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel) //微信提醒工单
            {
                //准备数据
                m_listTiOFsbdWxmsg.clear();

                AISTD string strQueueId = ltoa(m_imsNtfRemind.get_queueId());
                //注意：当工单量每秒超过百万条时，这种batchId拼接方式会出现重复
                AISTD string strAppBatchId = "ACT" + m_dtDateTime.toString("%Y%M%D%H%N%S") + strQueueId.substr(strQueueId.length() - 6); //账务为ACT+YYYYMMDDHHmmSS+6位序列号
                int32 iPartitionId = atoi(strAppBatchId.substr(strAppBatchId.length() - 3).c_str());

                AISTD string strTemplateId = "";
                AISTD string strMsgType = "";
                AISTD string strTemplateUrl = "";
                AISTD string strTemplateContent = "";
                int32 iMonth = 0;
                int32 iDay = 0;
                int64 llResourceId = m_imsNtfRemind.get_resourceId();
                AISTD string strRecvObject = m_imsNtfRemind.get_phoneId();
                char eparchyCode[5];
                sprintf(eparchyCode, "%04d", m_imsNtfRemind.get_regionCode());

                iMonth = atoi(m_dtDateTime.toString("%M").c_str());
                iDay = atoi(m_dtDateTime.toString("%D").c_str());

                //字段数量不足，提醒内容拼接在notifContent字段种
                LOG_TRACE("[WXMSG]=======>get_notifContent:%s", m_imsNtfCredit.get_notifContent().c_str());
                CStringList lstStrWithMsgType;
                CStringList lstWechatStr;
                int32 iMsgTypeSplitRet = split_msgType_notifyContent(m_imsNtfRemind.get_notifContent(), "|||", lstStrWithMsgType);
                if (iMsgTypeSplitRet == 2)
                {
                    strMsgType = lstStrWithMsgType.at(1);
                    base_divide_str(lstStrWithMsgType.at(0), "|", lstWechatStr);
                    if (lstWechatStr.size() == 2)
                    {
                        strTemplateContent = lstWechatStr.at(0);
                        strTemplateId = lstWechatStr.at(1);
                    }

                    if (lstWechatStr.size() == 3)
                    {
                        strTemplateContent = lstWechatStr.at(0);
                        strTemplateId = lstWechatStr.at(1);
                        strTemplateUrl = lstWechatStr.at(2);
                    }
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_imsNtfCredit.get_notifContent error");
                }
                LOG_TRACE("[WXMSG]=======>strMsgType:%s", strMsgType.c_str());

                MAbmInterfacePromptDb::CTiOFsbdWxmsg tiOFsbdWxmsg;
                tiOFsbdWxmsg.set_appBatchId(strAppBatchId);
                tiOFsbdWxmsg.set_partitionId(iPartitionId);
                tiOFsbdWxmsg.set_recvId(llResourceId);
                tiOFsbdWxmsg.set_recvObject(strRecvObject);
                tiOFsbdWxmsg.set_templateId(strTemplateId);
                tiOFsbdWxmsg.set_msgParams(strTemplateContent);
                tiOFsbdWxmsg.set_url(strTemplateUrl);
                tiOFsbdWxmsg.set_msgType(strMsgType);
                tiOFsbdWxmsg.set_sendType(0);
                tiOFsbdWxmsg.set_pushType(0);
                tiOFsbdWxmsg.set_pushLevel(1);
                tiOFsbdWxmsg.set_eparchyCode(eparchyCode);
                tiOFsbdWxmsg.set_referTime(m_dtDateTime);
                tiOFsbdWxmsg.set_dealTime(m_dtDateTime);
                tiOFsbdWxmsg.set_dealState(0);
                tiOFsbdWxmsg.set_month(iMonth);
                tiOFsbdWxmsg.set_day(iDay);

                m_listTiOFsbdWxmsg.push_back(tiOFsbdWxmsg);

                //入表
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertTiOFsbdWxmsg")
                retCode = insertTiOFsbdWxmsg(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            else if (m_smsTable.compare(TI_O_SMS) == 0)
            {
                m_listTiOSms.Clear();
                MAbmInterfacePromptDb::CTiOSms tiOSmsTmp;
                CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
                char eparchyCode[5];
                sprintf(eparchyCode, "0%d", m_imsNtfRemind.get_regionCode());
                // mod by ligc@20190418 短信夹带
                if ((!m_crmHttpServiceName.empty()) && (m_isSmsMarket) && (m_isCallSmsService))
                {
                    bSmsToTable = false;
                }
                if (bSmsToTable)
                {
                    /*tiOSmsTmp.set_smsNoticeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfRemind.get_queueId());
                    tiOSmsTmp.set_eparchyCode(eparchyCode);
                    tiOSmsTmp.set_inModeCode("0");
                    tiOSmsTmp.set_smsChannelCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
                    tiOSmsTmp.set_sendObjectCode(2);
                    tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                    tiOSmsTmp.set_sendCountCode(1);
                    tiOSmsTmp.set_recvObjectType("00");
                    tiOSmsTmp.set_recvObject(m_imsNtfRemind.get_phoneId());
                    tiOSmsTmp.set_id(m_imsNtfRemind.get_resourceId());
                    tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                    tiOSmsTmp.set_smsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                    tiOSmsTmp.set_noticeContentType("0");
                    tiOSmsTmp.set_noticeContent(m_imsNtfRemind.get_notifContent());
                    tiOSmsTmp.set_forceReferCount(1);
                    tiOSmsTmp.set_forceObject(m_imsNtfRemind.get_extend1()); //此处需要增加force_object
                    tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                    tiOSmsTmp.set_referTime(m_dtDateTime);
                    if (m_imsNtfRemind.get_notifChannelId() == m_pCfg->m_cfgCommon.m_iNotifyChann)
                    {
                        tiOSmsTmp.set_referStaffId("wechat");
                    }
                    else
                    {
                        tiOSmsTmp.set_referStaffId("remind");
                    }
                    tiOSmsTmp.set_referDepartId("CREDI");
                    tiOSmsTmp.set_dealTime(dealTime);
                    tiOSmsTmp.set_dealState("0");
                    //tiOSmsTmp.set_remindId(""+m_imsNtfRemind.get_notifChannelId()); //remind_id字段在短信表没有
                    //tiOSmsTmp.set_cycleTime(atoi(dtDateTime.toString("%Y%M").c_str()));  //cycle_time字段在短信表里没有
                    tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));*/

                    //add by xupp for yunnanV8 begin
                    tiOSmsTmp.set_sourceCode("02");
                    tiOSmsTmp.set_inModeCode("0");
                    tiOSmsTmp.set_sendObjectCode("01");
                    tiOSmsTmp.set_sendTimeCode(1);
                    tiOSmsTmp.set_smsTypeCode("0230");
                    tiOSmsTmp.set_smsPriority(2500);
                    tiOSmsTmp.set_referStaffId("REMIND00");
                    tiOSmsTmp.set_referDepartId("47336");
                    tiOSmsTmp.set_eparchyCode(eparchyCode);
                    tiOSmsTmp.set_receveObject(m_imsNtfRemind.get_phoneId());
                    tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));
                    tiOSmsTmp.set_day(atoi(m_dtDateTime.toString("%D").c_str()));
                    tiOSmsTmp.set_rsrvStr4("0");
                    tiOSmsTmp.set_tradeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfRemind.get_queueId());
                    int16 iPartitionId = tiOSmsTmp.get_tradeId() % TI_O_SMS_PARTITION_MOD;
                    tiOSmsTmp.set_partitionId(iPartitionId);
                    tiOSmsTmp.set_smsContent(m_imsNtfRemind.get_notifContent());
                    //m_listTiOSmsGprs.push_back(tiOSmsTmp);
                    //add by xupp for yunnanV8 end
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms route data : eparchy_code= %s,queue_id=%lld,acct_id = %lld",
                              tiOSmsTmp.get_eparchyCode().c_str(),
                              m_imsNtfRemind.get_queueId(),
                              m_imsNtfRemind.get_acctId());
                    m_listTiOSms.push_back(tiOSmsTmp);
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertIntoSms")
                    retCode = insertIntoSms(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                }
                else
                { // 调用服务
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->call_crmService")
                    retCode = call_crmService(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                    if (ABMPROMPT_OK != retCode)
                    { // 调用服务失败,需要将数据插入到TI_O_SMS表
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> call service fail, need insert to sms");
                        bIsCallServiceFail = true;
                        /*tiOSmsTmp.set_smsNoticeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfRemind.get_queueId());
                        tiOSmsTmp.set_eparchyCode(eparchyCode);
                        tiOSmsTmp.set_inModeCode("0");
                        tiOSmsTmp.set_smsChannelCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
                        tiOSmsTmp.set_sendObjectCode(2);
                        tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                        tiOSmsTmp.set_sendCountCode(1);
                        tiOSmsTmp.set_recvObjectType("00");
                        tiOSmsTmp.set_recvObject(m_imsNtfRemind.get_phoneId());
                        tiOSmsTmp.set_id(m_imsNtfRemind.get_resourceId());
                        tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                        tiOSmsTmp.set_smsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                        tiOSmsTmp.set_noticeContentType("0");
                        tiOSmsTmp.set_noticeContent(m_imsNtfRemind.get_notifContent());
                        tiOSmsTmp.set_forceReferCount(1);
                        tiOSmsTmp.set_forceObject(m_imsNtfRemind.get_extend1()); //此处需要增加force_object
                        tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                        tiOSmsTmp.set_referTime(m_dtDateTime);
                        if (m_imsNtfRemind.get_notifChannelId() == m_pCfg->m_cfgCommon.m_iNotifyChann)
                        {
                            tiOSmsTmp.set_referStaffId("wechat");
                        }
                        else
                        {
                            tiOSmsTmp.set_referStaffId("remind");
                        }
                        tiOSmsTmp.set_referDepartId("CREDI");
                        tiOSmsTmp.set_dealTime(dealTime);
                        tiOSmsTmp.set_dealState("0");
                        tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));*/
                        
                        //add by xupp for yunnanV8 begin
                        tiOSmsTmp.set_sourceCode("02");
                        tiOSmsTmp.set_inModeCode("0");
                        tiOSmsTmp.set_sendObjectCode("01");
                        tiOSmsTmp.set_sendTimeCode(1);
                        tiOSmsTmp.set_smsTypeCode("0230");
                        tiOSmsTmp.set_smsPriority(2500);
                        tiOSmsTmp.set_referStaffId("REMIND00");
                        tiOSmsTmp.set_referDepartId("47336");
                        tiOSmsTmp.set_eparchyCode(eparchyCode);
                        tiOSmsTmp.set_receveObject(m_imsNtfRemind.get_phoneId());
                        tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));
                        tiOSmsTmp.set_day(atoi(m_dtDateTime.toString("%D").c_str()));
                        tiOSmsTmp.set_rsrvStr4("0");
                        tiOSmsTmp.set_tradeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfRemind.get_queueId());
                        int16 iPartitionId = tiOSmsTmp.get_tradeId() % TI_O_SMS_PARTITION_MOD;
                        tiOSmsTmp.set_partitionId(iPartitionId);
                        tiOSmsTmp.set_smsContent(m_imsNtfRemind.get_notifContent());
                        //m_listTiOSmsGprs.push_back(tiOSmsTmp);
                        //add by xupp for yunnanV8 end
                        
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms route data : eparchy_code= %s,queue_id=%lld,acct_id = %lld",
                                  tiOSmsTmp.get_eparchyCode().c_str(),
                                  m_imsNtfRemind.get_queueId(),
                                  m_imsNtfRemind.get_acctId());
                        m_listTiOSms.push_back(tiOSmsTmp);
                        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertIntoSms")
                        retCode = insertIntoSms(pSession, cErrorMsg);
                        ES_END_RUN_TIME
                    }
                }
            }
            else if (m_smsTable.compare(TI_O_SMS_GPRS_REMIND) == 0)//可以用作定时提醒
            {
                m_listTiOSmsGprs.Clear();
                MAbmInterfacePromptDb::CTiOSmsGprsRemind tiOSmsGprsTmp;
                CBSDateTime dealTime(2050, 1, 1, 0, 0, 0);
                char eparchyCode[5];
                sprintf(eparchyCode, "%04d", m_imsNtfRemind.get_regionCode());
				AISTD string notifContent = m_imsNtfRemind.get_notifContent();
				
				//缅甸文工单特殊处理             
				if (m_imsNtfRemind.get_actionId() == 600000108 || m_imsNtfRemind.get_actionId() == 600000109 ||
					m_imsNtfRemind.get_actionId() == 600000110)
				{
					tiOSmsGprsTmp.set_rsrvStr4("2");
					replace_notifyContent(notifContent);
				}
				else
					tiOSmsGprsTmp.set_rsrvStr4("1");


                //add by xupp for yunnanV8 begin
                tiOSmsGprsTmp.set_sourceCode("02");
                tiOSmsGprsTmp.set_inModeCode("0");
                tiOSmsGprsTmp.set_sendObjectCode("01");
                tiOSmsGprsTmp.set_sendTimeCode(1);
                tiOSmsGprsTmp.set_smsTypeCode("0230");
                tiOSmsGprsTmp.set_referStaffId("REMIND00");
                tiOSmsGprsTmp.set_referDepartId("47336");
                tiOSmsGprsTmp.set_eparchyCode(eparchyCode);
                tiOSmsGprsTmp.set_receveObject(m_imsNtfRemind.get_phoneId());
                tiOSmsGprsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));
                tiOSmsGprsTmp.set_day(atoi(m_dtDateTime.toString("%D").c_str()));
                tiOSmsGprsTmp.set_smsContent(notifContent.c_str());
                tiOSmsGprsTmp.set_tradeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfRemind.get_queueId());
                int16 iPartitionId = tiOSmsGprsTmp.get_tradeId() % TI_O_SMS_PARTITION_MOD;
                tiOSmsGprsTmp.set_partitionId(iPartitionId);
                //add by xupp for yunnanV8 20210821 end
                    
                m_listTiOSmsGprs.push_back(tiOSmsGprsTmp);
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> the sms put into TI_O_SMS :  tradeId=%d, smsContent=%s",
                          tiOSmsGprsTmp.get_tradeId(), tiOSmsGprsTmp.get_smsContent().c_str());
                LOG_TRACE( "\n==[ABM_SERV_ABMPROMPT]==>eparchyCode=%s, tiOSmsGprsTmp.get_eparchyCode()=%s",
                          eparchyCode,tiOSmsGprsTmp.get_eparchyCode().c_str() );
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertIntoSmsGprs")
                retCode = insertIntoSmsGprs(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            else
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> invalid orders,m_smsTable:%s,m_nActionLevel:%d.",
                          m_smsTable, m_nActionLevel);
                retCode = ABMPROMPT_ERROR;
            }
        }

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->delete_inserthis")
        if (ABMPROMPT_OK != retCode && ABMPROMPT_ORDER_PHONE != retCode)
        {
            //contrl_rollback(pSession, cErrorMsg);
            m_imsNtfRemind.set_status(ABM_PROMPT_PROCESS_FAILED);
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfRemind.set_remark(strRemark.substr(0, 1024));
            m_imsNtfRemind.set_soDate(m_dtDateTime);
            update_data<MAbmInterfacePromptDb::CImsNtfRemindList>(pSession, cErrorMsg, m_imsNtfRemind);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld come from Remind sub_table ,%s : %s .",
                      m_imsNtfRemind.get_queueId(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
            //contrl_commit(pSession,cErrorMsg);
            continue;
        }

        MAbmInterfacePromptDb::CImsNtfRemindHis imsNtfRemindHis;
        transToHis(*m_itrImsNtfRemind, imsNtfRemindHis);
        imsNtfRemindHis.set_notifContent(m_imsNtfRemind.get_notifContent());
        if (ABMPROMPT_OK == retCode)
        {
            if (m_smsTable.compare(TI_O_SMS_GPRS_REMIND) == 0) //青海工单处理
            {
                imsNtfRemindHis.set_status(hisStatus);
                imsNtfRemindHis.set_remark("insert into ti_o_sms OK");
            }
            else //湖南工单处理
            {
                imsNtfRemindHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                if (bSmsToTable && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel && ENUM_ACTION_LEVEL_APP != m_nActionLevel)
                {
                    imsNtfRemindHis.set_remark("insert into ti_o_sms OK");
                }
                else if (bIsCallServiceFail && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel && ENUM_ACTION_LEVEL_APP != m_nActionLevel)
                {
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    strRemark.append("|insert into ti_o_sms OK");
                    imsNtfRemindHis.set_remark(strRemark.substr(0, 1024));
                }
                else if (ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel)
                {
                    imsNtfRemindHis.set_remark("insert into ti_o_fsbd_wxmsg OK");
                }
                else if (ENUM_ACTION_LEVEL_APP == m_nActionLevel)
                {
                    imsNtfRemindHis.set_remark("insert into ti_o_fsbd_appmsg OK");
                }
                else
                {
                    imsNtfRemindHis.set_remark((cErrorMsg.get_errorMsg() + ":" + cErrorMsg.get_hint()).substr(0, 1024));
                }
            }
        }
        else
        {
            imsNtfRemindHis.set_status(ABM_PROMPT_PROCESS_PHONE_FILTER);
            imsNtfRemindHis.set_remark((cErrorMsg.get_errorMsg() + ":" + cErrorMsg.get_hint()).substr(0, 1024));
        }

        imsNtfRemindHis.set_soDate(m_dtDateTime);
        imsNtfRemindHis.set_tfDate(CBSDateTime::currentDateTime()); //工单搬入历史表的时间
        if (strExternSms != "")
        {
            imsNtfRemindHis.set_extend1(strExternSms);
        }

        m_listImsNtfRemindHis.push_back(imsNtfRemindHis);

        if (insert_dataToHisRemind(pSession, cErrorMsg) == ABMPROMPT_OK)
        {
            if (delete_data<MAbmInterfacePromptDb::CImsNtfRemindList>(cErrorMsg, m_imsNtfRemind) != ABMPROMPT_OK)
            {
                contrl_rollback(pSession, cErrorMsg);
                m_imsNtfRemind.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfRemind.set_remark(strRemark.substr(0, 1024));
                m_imsNtfRemind.set_soDate(m_dtDateTime);

                update_data<MAbmInterfacePromptDb::CImsNtfRemindList>(pSession, cErrorMsg, m_imsNtfRemind);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld come from Remind sub_table ,%s : %s .",
                          m_imsNtfRemind.get_queueId(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
                contrl_commit(pSession, cErrorMsg);
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from remind sub_table is done successed",
                          m_imsNtfRemind.get_queueId(),
                          m_imsNtfRemind.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
            }
        }
        else
        {
            contrl_rollback(pSession, cErrorMsg);
            m_imsNtfRemind.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfRemind.set_remark(strRemark.substr(0, 1024));
            m_imsNtfRemind.set_soDate(m_dtDateTime);

            update_data<MAbmInterfacePromptDb::CImsNtfRemindList>(pSession, cErrorMsg, m_imsNtfRemind);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld come from Remind sub_table ,%s : %s .",
                      m_imsNtfRemind.get_queueId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
            contrl_commit(pSession, cErrorMsg);
        }

        ES_END_RUN_TIME

        ES_END_RUN_TIME

        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }

        ABM_PROMPT_STAT
    }
    contrl_commit(pSession, cErrorMsg);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> loop process success : %d orders processed. ", m_listImsNtfRemind.size());
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_service(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_service->query_data")
    query_data(m_listImsNtfService);
    ES_END_RUN_TIME

    if (m_listImsNtfService.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        if (m_isNeedUpdateStatus)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfServList, MAbmInterfacePromptDb::CImsNtfServ>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfService is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_service order : %d orders from table %s.", m_listImsNtfService.size(), m_sourceTable.c_str());

    int32 iCount = 0;
    for (m_itrImsNtfService = m_listImsNtfService.begin(); m_itrImsNtfService != m_listImsNtfService.end(); ++m_itrImsNtfService)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_service->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfService = *m_itrImsNtfService;
        m_actionId = m_itrImsNtfService->get_actionId();
        m_listImsNtfServiceHis.clear();
        m_listImsNtfServiceHisRate.clear();

        int32 retCode = ABMPROMPT_OK;
        if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
        {
            AISTD string strErrMsg("action_id = ");
            strErrMsg.append(cdk::strings::Itoa(m_actionId));
            strErrMsg.append("  get action_id info failed!");
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
            cErrorMsg.set_hint(strErrMsg);
            retCode = ABMPROMPT_ERROR;
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
        }

        if (ABMPROMPT_OK == retCode)
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> call crm to  change service status data : region_code= %d,queue_id=%lld,acct_id = %lld",
                      m_imsNtfService.get_regionCode(),
                      m_imsNtfService.get_queueId(),
                      m_imsNtfService.get_acctId());
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_service->call_crmService")
            retCode = call_crmService(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }

        m_dtDateTime = CBSDateTime::currentDateTime();
        if (m_isNeedUpdateStatus &&
            cErrorMsg.get_errorMsg() != "-100" &&
            cErrorMsg.get_errorMsg() != "-101" &&
            cErrorMsg.get_errorMsg() != "-102" &&
            cErrorMsg.get_errorMsg() != "-103")
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfServList, MAbmInterfacePromptDb::CImsNtfServ>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_service->delete_inserthis")
        if (ABMPROMPT_OK != retCode)
        {
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfService.set_remark(strRemark.substr(0, 1024));
            m_imsNtfService.set_soDate(m_dtDateTime);

            m_imsNtfService.set_status(ABM_PROMPT_PROCESS_FAILED); //处理错误，需要手工处理
            if (cErrorMsg.get_errorMsg() == "-100" ||
                cErrorMsg.get_errorMsg() == "-101" ||
                cErrorMsg.get_errorMsg() == "-102" ||
                cErrorMsg.get_errorMsg() == "-103")
            {
                if (m_imsNtfService.get_stateDtlId() < OTHER_START_STATE + m_iUpdateCrmExcep)
                {
                    m_imsNtfService.set_status(atoi(cErrorMsg.get_errorMsg().c_str()));
                    m_imsNtfService.set_stateDtlId(m_imsNtfService.get_stateDtlId() + 1);
                    m_isNeedUpdateStatus = true;
                }
                else
                {
                    m_imsNtfService.set_stateDtlId(OTHER_START_STATE);
                }
            }
            update_data<MAbmInterfacePromptDb::CImsNtfServList>(pSession, cErrorMsg, m_imsNtfService);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Service sub_table, %s : %s",
                      m_imsNtfService.get_queueId(),
                      m_imsNtfService.get_acctId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfServHis imsNtfServiceHis;
            MAbmInterfacePromptDb::CImsNtfServRate imsNtfServiceHisRate;
            transToHis(*m_itrImsNtfService, imsNtfServiceHis);
            transToHis(*m_itrImsNtfService, imsNtfServiceHisRate);
            if (ABMPROMPT_OK == retCode)
            {
                imsNtfServiceHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                imsNtfServiceHisRate.set_status(ABM_PROMPT_PROCESS_SUCCESS);
            }
            else
            {
                imsNtfServiceHis.set_status(ABM_PROMPT_PROCESS_FAILED);
            }
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            imsNtfServiceHis.set_remark(strRemark.substr(0, 1024));
            imsNtfServiceHis.set_soDate(m_dtDateTime);
            m_listImsNtfServiceHis.push_back(imsNtfServiceHis);

            //移动商城====begin====================
            aistring strtmp = "";
            int32 iRet = get_sysParamter("LIMIT_SPEED_SERVICE_FLAG", strtmp);
            if (ABMPROMPT_OK == iRet && ABMPROMPT_OK == retCode)
            {
                CStringList notificationIdList;
                cdk::strings::Split(strtmp, "|", notificationIdList);
                for (CStringList::iterator it = notificationIdList.begin(); it != notificationIdList.end(); ++it)
                {
                    if (m_itrImsNtfService->get_notificationId() == atoi((*it).c_str()))
                    {
                        imsNtfServiceHisRate.set_remark(strRemark.substr(0, 1024));
                        imsNtfServiceHisRate.set_soDate(m_dtDateTime);
                        m_listImsNtfServiceHisRate.push_back(imsNtfServiceHisRate);
                        break;
                    }
                }
            }
            //============end======================

            if (insert_dataToHisService(pSession, cErrorMsg) == ABMPROMPT_OK)
            {
                if (delete_data<MAbmInterfacePromptDb::CImsNtfServList>(cErrorMsg, m_imsNtfService) != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    m_imsNtfService.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfService.set_soDate(m_dtDateTime);
                    m_imsNtfService.set_remark(strRemark.substr(0, 1024));
                    update_data<MAbmInterfacePromptDb::CImsNtfServList>(pSession, cErrorMsg, m_imsNtfService);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Service sub_table, %s : %s",
                              m_imsNtfService.get_queueId(),
                              m_imsNtfService.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from service sub_table is done successed",
                              m_imsNtfService.get_queueId(),
                              m_imsNtfService.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                }
            }
            else
            {
                //contrl_rollback(pSession, cErrorMsg);
                m_imsNtfService.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfService.set_remark(strRemark.substr(0, 1024));
                m_imsNtfService.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfServList>(pSession, cErrorMsg, m_imsNtfService);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Service sub_table, %s : %s",
                          m_imsNtfService.get_queueId(),
                          m_imsNtfService.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
                //contrl_commit(pSession, cErrorMsg);
            }
        }

        ES_END_RUN_TIME

        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME
        ABM_PROMPT_STAT
    }
    contrl_commit(pSession, cErrorMsg);
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_confirm(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->query_data")
    query_data(m_listImsNtfConfirm);
    ES_END_RUN_TIME

    if (m_listImsNtfConfirm.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        if (m_isNeedUpdateStatus)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfConfList, MAbmInterfacePromptDb::CImsNtfConf>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfConfirm is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_confirm order : %d orders from table %s.", m_listImsNtfConfirm.size(), m_sourceTable.c_str());
    int32 iCount = 0;
    for (m_itrImsNtfConfirm = m_listImsNtfConfirm.begin(); m_itrImsNtfConfirm != m_listImsNtfConfirm.end(); m_itrImsNtfConfirm++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfConfirm = *m_itrImsNtfConfirm;
        m_actionId = m_itrImsNtfConfirm->get_actionId();
        m_listImsNtfConfirmHis.clear();

        int32 retCode = ABMPROMPT_OK;
        m_notifCont.init();

        SYSTEM_BASE_TYPE_HUNAN_IF
        retCode = parseNotifContent(m_imsNtfConfirm.get_notifContent(), m_notifCont);
        SYSTEM_BASE_TYPE_QINHAI_ELIF
        retCode = parseNotifContent(m_imsNtfConfirm.get_extend1(), m_notifCont);
        SYSTEM_BASE_TYPE_END

        if (retCode != ABMPROMPT_OK)
        {
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ORDER_INCORRECT));
            cErrorMsg.set_hint("parse notif_content failed!");
        }

        //判断该批次是否已经调用过ESOP
        bool isExistFlag = false;
        if (ABMPROMPT_OK == retCode)
        {
            retCode = checkSeriesId(pSession, m_imsNtfConfirm, cErrorMsg);
        }

        if (ABMPROMPT_OK == retCode) //查询的结果为空，未调用过ESOP,需要调用ESOP接口
        {
            //esop单独使用特殊的action_id，该action_id约定好，限制死。天津特殊。
            m_actionId = 388888888;
            retCode = get_actionExtInfo(pSession, cErrorMsg);
            if (retCode != ABMPROMPT_OK)
            {
                AISTD string strErrMsg("action_id = ");
                strErrMsg.append(cdk::strings::Itoa(m_actionId));
                strErrMsg.append("  get action_id info failed!");
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
                cErrorMsg.set_hint(strErrMsg);
                retCode = ABMPROMPT_ERROR;
                LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
            }

            if (ABMPROMPT_OK == retCode)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> call esop confirm data : eparchy_code= %d,queue_id=%lld,acct_id = %lld",
                          m_imsNtfConfirm.get_regionCode(),
                          m_imsNtfConfirm.get_queueId(),
                          m_imsNtfConfirm.get_acctId());
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->call_crmService")
                retCode = call_crmService(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
        }
        else if (ABMPROMPT_DONE == retCode) //该批次已经调用成功过
        {
            isExistFlag = true;
            LOG_TRACE(" == [ABM_SERV_ABMPROMPT] == > find batch_id = %lld in Ti_O_CreditWork_Cust , the queue_id = %lld",
                      m_imsNtfConfirm.get_seriesId(),
                      m_imsNtfConfirm.get_queueId());
        }

        m_dtDateTime = CBSDateTime::currentDateTime();
        if (ABMPROMPT_OK == retCode || ABMPROMPT_DONE == retCode)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->insert_cust")
            MAbmInterfacePromptDb::CTiOCreditworkCust sCreditWorkCust;
            trans_confirm(m_imsNtfConfirm, m_notifCont, m_dtDateTime, sCreditWorkCust);
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> insert TI_O_CREDITWORK_CUST as follow: %s",
                      sCreditWorkCust.to_string().c_str());
            retCode = insertCreditWorkCust(pSession, sCreditWorkCust, cErrorMsg);
            ES_END_RUN_TIME
        }

        if (m_isNeedUpdateStatus &&
            cErrorMsg.get_errorMsg() != "-100" &&
            cErrorMsg.get_errorMsg() != "-101" &&
            cErrorMsg.get_errorMsg() != "-102" &&
            cErrorMsg.get_errorMsg() != "-103")
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfConfList, MAbmInterfacePromptDb::CImsNtfConf>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->delete_inserthis")
        if (ABMPROMPT_OK != retCode)
        {
            //contrl_rollback(pSession, cErrorMsg);
            if (ABMPROMPT_DB_OP_ERR == retCode)
            {
                m_imsNtfConfirm.set_status(ABM_PROMPT_PROCESS_CUSTFAILED); // 操作cust工单表失败
            }
            else
            {
                m_imsNtfConfirm.set_status(ABM_PROMPT_PROCESS_FAILED); // 调esop失败
                if (cErrorMsg.get_errorMsg() == "-100" ||
                    cErrorMsg.get_errorMsg() == "-101" ||
                    cErrorMsg.get_errorMsg() == "-102" ||
                    cErrorMsg.get_errorMsg() == "-103")
                {
                    if (m_imsNtfConfirm.get_stateDtlId() < STS_START_STATE + m_iUpdateCrmExcep)
                    {
                        m_imsNtfConfirm.set_status(atoi(cErrorMsg.get_errorMsg().c_str()));
                        m_imsNtfConfirm.set_stateDtlId(m_imsNtfConfirm.get_stateDtlId() + 1);
                        m_isNeedUpdateStatus = true;
                    }
                    else
                    {
                        m_imsNtfConfirm.set_status(ABM_PROMPT_PROCESS_FAILED);
                        m_imsNtfConfirm.set_stateDtlId(OTHER_START_STATE);
                    }
                }
            }

            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfConfirm.set_remark(strRemark.substr(0, 1024));
            m_imsNtfConfirm.set_soDate(m_dtDateTime);
            //m_imsNtfConfirm.set_status(ABM_PROMPT_PROCESS_FAILED);

            update_data<MAbmInterfacePromptDb::CImsNtfConfList>(pSession, cErrorMsg, m_imsNtfConfirm);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table,%s : %s.",
                      m_imsNtfConfirm.get_queueId(),
                      m_imsNtfConfirm.get_acctId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
            //contrl_commit(pSession,cErrorMsg);
            //continue;
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfConfHis imsNtfConfirmHis;
            transToHis(*m_itrImsNtfConfirm, imsNtfConfirmHis);
            imsNtfConfirmHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
            if (isExistFlag) //该批次已经执行过调用ESOP
            {
                imsNtfConfirmHis.set_remark("This batch has called ESOP successfully,so this record don't need to call ESOP again.");
            }
            else
            {
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                imsNtfConfirmHis.set_remark(strRemark.substr(0, 1024));
            }

            imsNtfConfirmHis.set_soDate(m_dtDateTime);
            m_listImsNtfConfirmHis.push_back(imsNtfConfirmHis);

            if (insert_dataToHisConfirm(pSession, cErrorMsg) == ABMPROMPT_OK)
            {
                if (delete_data<MAbmInterfacePromptDb::CImsNtfConfList>(cErrorMsg, m_imsNtfConfirm) != ABMPROMPT_OK)
                {
                    //contrl_rollback(pSession, cErrorMsg);
                    m_imsNtfConfirm.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED); // -1
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfConfirm.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfConfirm.set_soDate(m_dtDateTime);
                    update_data<MAbmInterfacePromptDb::CImsNtfConfList>(pSession, cErrorMsg, m_imsNtfConfirm);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table,%s : %s.",
                              m_imsNtfConfirm.get_queueId(),
                              m_imsNtfConfirm.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                    //contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table, do successfully",
                              m_imsNtfConfirm.get_queueId(),
                              m_imsNtfConfirm.get_acctId());
                }
            }
            else
            {
                m_imsNtfConfirm.set_status(ABM_PROMPT_PROCESS_MOVEFAILED); // 4
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfConfirm.set_remark(strRemark.substr(0, 1024));
                m_imsNtfConfirm.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfConfList>(pSession, cErrorMsg, m_imsNtfConfirm);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table,%s : %s.",
                          m_imsNtfConfirm.get_queueId(),
                          m_imsNtfConfirm.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
            }
        }
        ES_END_RUN_TIME
        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    contrl_commit(pSession, cErrorMsg);
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::parseNotifContent(const AISTD string &strNotifContent, Notifcontent &sNotif)
{
    //notif_content为短信内容，改为解析extend1字段, 格式为"TRADE_EPARCHY_CODE|:xxxxx;TRADE_STAFF_ID|yyyy;TRADE_DEPART_ID|zzzz"
    vector<string> vct_notifContent;
    map<string, string> map_notifContent;
    vector<string> key_value;
    cdk::strings::Split(strNotifContent, ";", vct_notifContent);
    for (vector<string>::iterator itr = vct_notifContent.begin(); itr != vct_notifContent.end(); itr++)
    {
        cdk::strings::Split(*itr, "|", key_value);
        if (key_value.size() != 2)
        {
            LOG_ERROR(ABMPROMPT_ORDER_INCORRECT, "\n==[ABM_SERV_ABMPROMPT]==> notif_cotent parase error!========");
            return -1;
        }
        map_notifContent.insert(map<string, string>::value_type(::trimBoth(key_value[0]), ::trimBoth(key_value[1])));
    }
    /*
       5304 TRADE_EPARCHY_CODE
       5305 TRADE_CITY_CODE
       5303 ASSIGN_STAFF_ID
       5306 ASSIGN_DEPART_ID 
       5307 BATCH_ID
       5308 CONTROL_TYPE
       5309 CUST_ID 
       UPDATE_DEPART_ID 写死
     */
    if (map_notifContent.find("5304") != map_notifContent.end())
    {
        sNotif.tradeEparchyCode = map_notifContent["5304"];
    }
    if (map_notifContent.find("5305") != map_notifContent.end())
    {
        sNotif.tradeCityCode = map_notifContent["5305"];
    }
    if (map_notifContent.find("5303") != map_notifContent.end())
    {
        sNotif.assignStaffid = map_notifContent["5303"];
    }
    if (map_notifContent.find("5306") != map_notifContent.end())
    {
        sNotif.assignDepartid = map_notifContent["5306"];
    }
    if (map_notifContent.find("5307") != map_notifContent.end())
    {
        sNotif.batchId = map_notifContent["5307"];
    }
    if (map_notifContent.find("5308") != map_notifContent.end())
    {
        sNotif.controlType = map_notifContent["5308"];
    }
    if (map_notifContent.find("5309") != map_notifContent.end())
    {
        sNotif.cust_id = cdk::strings::Atol64(map_notifContent["5309"]);
    }
    //UPDATE_STAFF_ID、UPDATE_DEPART_ID写死为CREDIT00、CREDI
    sNotif.updateStaffid = "CREDIT00";
    sNotif.updateDepartid = "CREDI";

    if (sNotif.checkEmpty())
    {
        LOG_ERROR(ABMPROMPT_ORDER_INCORRECT, "\n====[ABM_SERV_ABMPROMPT]===>  notif_cotent parase error, as follow:\n {%s}", sNotif.to_string().c_str());
        return ABMPROMPT_ERROR;
    }
    return ABMPROMPT_OK;
}

//单个处理sts状态表数据
int32 ABMPrompt::deal_singleSts(SOBSession *pSession, const ::MAbmInterfaceAbmpromptDef::SPromptSts &sIn, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    if (get_cfg(pSession, cErrorMsg) == -1)
    {
        return ABMPROMPT_ERROR;
    }
    int32 iRetValue = ABMPROMPT_OK;
    m_strAppType = ABM_PROMPT_APP_TYPE_STS;
    //陕西的工单表表名里添加了地市编码
    AISTD string strRegionCode = AISTD string(m_pCfg->m_cfgParams.m_szRegionCode);
    m_hisTable = "IMS_NTF_STS_HIS_" + strRegionCode + "_";
    int64 llSequenceId = 9999999999;

    m_actionId = sIn.get_actionId();
    m_imsNtfSts.set_queueId(llSequenceId);
    m_imsNtfSts.set_acctId(sIn.get_acctId());
    m_imsNtfSts.set_resourceId(sIn.get_resourceId());
    m_imsNtfSts.set_regionCode(sIn.get_regionCode());
    m_imsNtfSts.set_phoneId(sIn.get_phoneId());
    m_imsNtfSts.set_stateId(sIn.get_stateId());
    m_imsNtfSts.set_remark("Emergency  open called by ca_notif_queue base table！");
    char t_buff[100];
    sprintf(t_buff, "%s,%s", sIn.get_updateStaffId().c_str(), sIn.get_updateDepartId().c_str());
    m_imsNtfSts.set_extend1(t_buff);

    //获取接口地址等信息
    if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
    {
        AISTD string strErrMsg("action_id = ");
        strErrMsg.append(cdk::strings::Itoa(m_actionId));
        strErrMsg.append("  get action_id info failed!");
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
        cErrorMsg.set_hint(strErrMsg);
        iRetValue = ABMPROMPT_ERROR;
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
        return ABMPROMPT_ERROR;
    }

    //调用接口，根据接口返回码做不同的处理
    if (iRetValue == ABMPROMPT_OK)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->deal_singleSts->call_crmService")
        iRetValue = call_crmService(pSession, cErrorMsg);
        ES_END_RUN_TIME
    }

    m_dtDateTime = CBSDateTime::currentDateTime();
    if (ABMPROMPT_OK == iRetValue || ABMPROMPT_REPEATED == iRetValue)
    {
        try
        {
            if (ABMPROMPT_OK == iRetValue)
            {
                llSequenceId = static_cast<int64>(ob_kernelBase::get_sequence_value("JD.IMS_NTF_SEQ"));
                //生成历史表工单
                m_listImsNtfStsHis.clear();
                m_imsNtfSts.set_queueId(llSequenceId);
                MAbmInterfacePromptDb::CImsNtfStsHis imsNtfStsHis;
                trans2ImsNtf(m_imsNtfSts, m_dtDateTime, imsNtfStsHis);
                imsNtfStsHis.set_alarmSourceType(8);
                m_listImsNtfStsHis.push_back(imsNtfStsHis);
                insert_dataToHisSts(pSession, cErrorMsg);
            }
        }
        catch (err_info_service::CAIException &e)
        {
            LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , catch exception: Code: %lld, message: %s, logrecord: %s",
                      __func__, e.get_code(), e.get_message().c_str(), e.get_logrecord().c_str());
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
            cErrorMsg.set_hint(e.get_message());
            return ABMPROMPT_DB_OP_ERR;
        }
        catch (otl_exception &e)
        {

            AISTD string strOtl = AISTD string("error_msg=\notl_exception:") + AISTD string((char *)e.msg) + AISTD string(e.stm_text) + AISTD string(e.var_info);
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
            cErrorMsg.set_hint(strOtl);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==>func:%s , Exception: %s", __func__, strOtl.c_str());
            return ABMPROMPT_DB_OP_ERR;
        }
        catch (...)
        {
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
            cErrorMsg.set_hint("insert ims_ntf_sts_his catch unknown exception ");
            LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , catch unknown exception!!!========", __func__);
            return ABMPROMPT_DB_OP_ERR;
        }
        //调用方统一提交
        //contrl_commit(pSession, cErrorMsg);
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> acct_id =%lld ,resource_id = %lld, action_id = %d call crm successed！",
                  sIn.get_acctId(), sIn.get_resourceId(), sIn.get_actionId());
    }
    else if (iRetValue == 1) //CRM侧返回需要降速，CRM错误码：CRM_TRADECREDIT_999
    {
        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> acct_id =%lld , action_id = %d call crm failed！, return  %d",
                  sIn.get_acctId(), sIn.get_actionId(), iRetValue);
    }
    else
    {
        LOG_ERROR(0, "\n == [ABM_SERV_ABMPROMPT] == > acct_id = %lld, action_id = %d call crm failed , return  %d",
                  sIn.get_acctId(), sIn.get_actionId(), iRetValue);
    }

    LEAVE_FUNC
    return iRetValue;
}

int32 ABMPrompt::qurey_infoCrm(SOBSession* pSession, const ::MAbmInterfaceAbmpromptDef::SQueryCustIn& sIn, ::MAbmInterfaceAbmpromptDef::SQueryCustOut& sOut, CBSErrorMsg& cErrorMsg)
{
	ENTER_FUNC
	if (get_cfg(pSession, cErrorMsg) == -1)
	{
		return ABMPROMPT_ERROR;
	}
	int32 iRet = SDL_OK;
	LOG_TRACE("\n==[ABM_SERV_PROMPT]==>%s's input data as follow:%s",__func__,sIn.to_string().c_str());

	if (sIn.get_qureyFlag()>QUERY_CUST_ID)
	{
		LOG_ERROR(0, "\n==[ABM_SERV_PROMPT]==> The query_flag = %d is undefined,pleas check!", sIn.get_qureyFlag());
		return SDL_FAIL;
	}

	//xc�ж�ȡ��������
	if (sIn.get_qureyFlag()==QUERY_CUST_NAME)
	{
		if (SDL_FAIL==get_crmService(pSession, CALL_CRM_SERVICE_CUST_NAME, cErrorMsg))
		{
			m_crmHttpServiceName = "CS.CustomerRelaQry.qryCustName";
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> m_crmHttpServiceName = %s as default, when query_flag = %d ", 
				m_crmHttpServiceName.c_str(), 
				sIn.get_qureyFlag());
		}
	}
	else if (sIn.get_qureyFlag() == QUERY_ACCT_NAME)
	{
		if (SDL_FAIL == get_crmService(pSession, CALL_CRM_SERVICE_ACCT_NAME, cErrorMsg))
		{
			m_crmHttpServiceName = "CS.AccountRelaQry.qryPayName";
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> m_crmHttpServiceName = %s as default, when query_flag = %d ",
				m_crmHttpServiceName.c_str(),
				sIn.get_qureyFlag());
		}
	}
	else if (sIn.get_qureyFlag() == QUERY_PSPT_ID)
	{
		if (SDL_FAIL == get_crmService(pSession, CALL_CRM_SERVICE_PSPT_INFO, cErrorMsg))
		{
			m_crmHttpServiceName = "CS.CustomerRelaQry.qryPsptId";
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> m_crmHttpServiceName = %s as default, when query_flag = %d ",
				m_crmHttpServiceName.c_str(),
				sIn.get_qureyFlag());
		}
	}
	else if (sIn.get_qureyFlag() == QUERY_CUST_ID)
	{
		if(SDL_FAIL == get_crmService(pSession, CALL_CRM_SERVICE_CUST_ID, cErrorMsg))
		{
			m_crmHttpServiceName = "SS.CustomerRelaQrySVC.qryCustomerByPsptId";
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> m_crmHttpServiceName = %s as default, when query_flag = %d ",
				m_crmHttpServiceName.c_str(),
				sIn.get_qureyFlag());
		}
	}

	if (get_svcURI(pSession,cErrorMsg)!=ABMPROMPT_OK)
	{
		LOG_ERROR(ABMPROMPT_XC_ERROR, "====[ABM_SERV_ABMPROMPT]===> get_svcURI failed");
		return ABMPROMPT_ERROR;
	}

	aistring strCustId = cdk::strings::Itoa(sIn.get_custId());
	aistring acct_name("");
	aistring cust_name("");
	aistring psptid("");
	aistring pstType("");
	aistring retInfo("");

    //qiankun3 20190705 crm��ȫ��־ ��ȡ����ID
	string sLoginIp = "";
	string sRemoteAddr = "";
    string sClientIp = "";
	if(ABMPROMPT_OK == getLocalIp(sLoginIp))
	{
		sRemoteAddr = sLoginIp;
        sClientIp = sLoginIp;
	}
	LOG_TRACE("LgoinIp:%s",sLoginIp.c_str());
    
	CALL_CRM_RETRY_BEGIN
		m_strCallCrmResultCode = m_crmCaller.queryCustAcctInfoCrm(m_svcURI,
			m_crmHttpServiceName,
			sIn.get_qureyFlag(),
			sIn.get_acctId(),
			sIn.get_regionCode(),
			strCustId,
			acct_name,
			cust_name,
			psptid,
			pstType,
			sLoginIp,
			sRemoteAddr,
			sClientIp,
			retInfo);
	CALL_CRM_RETRY_END

	cErrorMsg.set_errorMsg(m_strCallCrmResultCode);
	cErrorMsg.set_hint(retInfo);
	LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===>result_code =  %s, retInof = %s", m_strCallCrmResultCode.c_str(), retInfo.c_str());
	if (m_strCallCrmResultCode!="0")
	{
		iRet = SDL_FAIL;
		LOG_ERROR(0, "====[ABM_SERV_ABMPROMPT]===> query crm info  failed:return=%s,result info=%s",
			m_strCallCrmResultCode.c_str(),
			retInfo.c_str());
	}
	else
	{
		if (sIn.get_qureyFlag() == QUERY_CUST_NAME)
		{
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> cust_name = %s",cust_name.c_str());
			sOut.set_custName(cust_name);
		}
		else if (sIn.get_qureyFlag() == QUERY_ACCT_NAME)
		{
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> acct_name = %s", acct_name.c_str());
			sOut.set_acctName(acct_name);
		}
		else if (sIn.get_qureyFlag() == QUERY_PSPT_ID)
		{
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> psptid = %s,pstType= %s", psptid.c_str(), pstType.c_str());
			sOut.set_psptId(psptid);
			sOut.set_psptTypeCode(pstType);
		}
		else if (sIn.get_qureyFlag() == QUERY_CUST_ID)
		{
			LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> cust_id = %s", strCustId.c_str());
			int64 cust_id = 0;
			if (!strCustId.empty())
			{
				cust_id = atol64(strCustId.c_str());
			}
			sOut.set_custId(cust_id);
		}

		LOG_TRACE("\n====[ABM_SERV_ABMPROMPT]===> ouput is as follow:\n%s", sOut.to_string().c_str());
	}

	LEAVE_FUNC
	return iRet;
}

//替换短信中的：[URT]
int32 ABMPrompt::replaceRemindContent(const aistring &strCont, const aistring &strReplaceTemp, const aistring &strReplaceCont, aistring &strContResult)
{
    ENTER_FUNC
    strContResult = strCont;
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need replace str = %s, replaced_str = %s,repalce_str =%s ", strCont.c_str(), strReplaceTemp.c_str(), strReplaceCont.c_str());
    if (strReplaceTemp.empty() || strContResult.empty())
    {
        return ABMPROMPT_OK;
    }
    size_t bpos = strContResult.find(strReplaceTemp);
    if (bpos != std::string::npos)
    {
        size_t lenCont = strReplaceTemp.size();
        strContResult.replace(bpos, lenCont, strReplaceCont);
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> src_content = %s, replaced_content=%s, repalce_content =%s , dest_content = %s",
                  strCont.c_str(),
                  strReplaceTemp.c_str(),
                  strReplaceCont.c_str(),
                  strContResult.c_str());
    }
    else
    {
        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> the src_content = {%s},can't find '%s',please check!", strCont.c_str(), strReplaceTemp.c_str());
        return ABMPROMPT_ERROR;
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}
//校验手机号，必须为11位，且第一位为'1'
bool ABMPrompt::check_phoneId(const AISTD string &strPhoneId)
{
    if (strPhoneId.size() != 11 || strPhoneId.at(0) != '1' || "1064" == strPhoneId.substr(0,4))
    {
        return false;
    }
    return true;
}

//查找"<>"，查询到返回false，说明有param_id未替换完成
bool ABMPrompt::check_notifyContent(const AISTD string &strNotifyContent)
{
    aistring str = "<>";
    for (aistring::iterator it = str.begin(); it != str.end(); ++it)
    {
        if (strNotifyContent.find(*it) != aistring::npos)
        {
            return false;
        }
    }
    return true;
}

//缅甸文短信内容特殊处理
bool ABMPrompt::replace_notifyContent(AISTD string &strNotifyContent)
{
    aistring flag = "|";
	aistring str1,str2;
	int num = 0;

	LOG_TRACE("original notify_content:%s",strNotifyContent.c_str());
	for (size_t i = 0; (i = strNotifyContent.find(flag, i)) != std::string::npos; num++, i++);
	if (num == 3)
	{
		LOG_TRACE("replace notify_content");
		int pos = strNotifyContent.rfind(flag);
	    str1 = strNotifyContent.substr(0,pos);
		str2 = 	strNotifyContent.substr(pos);		
		strNotifyContent.clear();
		strNotifyContent = str1 + "|0GB";
		strNotifyContent += str2;
		strNotifyContent += "|0GB";		
	}
	LOG_TRACE("final notify_content:%s",strNotifyContent.c_str());
}

int32 ABMPrompt::get_sysParamter(
    const AISTD string &strParam,
    AISTD string &strValue)
{
    LOG_TRACE("\n==[ABM_NOTI_FILTER]==>  get_sysParamter begin  strParam = %s, ", strParam.c_str());
    // SD.SYS_PARAMTER
    xc::CSnapshot cSnap("ZW::ABM_BALANCE");
    xc::CQueryHolder<ZW::ABM_BALANCE::CAbmSysParameter::Type> cSysQueryHolder(cSnap, ZW::ABM_BALANCE::CAbmSysParameter::GetContainerName());
    ZW::ABM_BALANCE::CAbmSysParameter::Type::iterator iter_sys = cSysQueryHolder.GetContainer().find(strParam);

    if (iter_sys != cSysQueryHolder.GetContainer().end())
    {
        strValue = iter_sys->second.GetParamValue();
        LOG_TRACE("find %s from SYS_PARAMTER param_value = %s", strParam.c_str(), strValue.c_str());
    }
    else
    {
        LOG_TRACE("can not find %s from SYS_PARAMTER param_value = %s", strParam.c_str(), strValue.c_str());
        return ABMPROMPT_ERROR;
    }

    LOG_TRACE("\n==[ABM_NOTI_FILTER]==>  get_sysParamter end  strValue= %s, ", strValue.c_str());
    return ABMPROMPT_OK;
}

//以分割符分割短信
int32 ABMPrompt::divide_sms_str(const AISTD string &in_str, const AISTD string &in_divideStr, CStringList *out_list)
{
    AISTD string strBuf;
    AISTD string::size_type begin_pos = 0, end_pos = 0, i = 0;

    if (!out_list->empty())
        out_list->clear();

    if (in_str.empty())
        return 0;

    if (in_divideStr.empty())
    {
        out_list->push_back(in_str);
        return 1;
    }

    while (begin_pos < in_str.length())
    {
        end_pos = in_str.find(in_divideStr, begin_pos);
        if (end_pos == AISTD string::npos)
        {
            strBuf = in_str.substr(begin_pos, in_str.length() - begin_pos);
            begin_pos = in_str.length();
        }
        else
        {
            if (begin_pos == end_pos)
                strBuf = "";
            else
                strBuf = in_str.substr(begin_pos, end_pos - begin_pos);
            begin_pos = end_pos + in_divideStr.length();
        }
        if ((!strBuf.empty()) && (strBuf.length() > 0))
            out_list->push_back(strBuf);
    }

    return out_list->size();
}

//以长度分割短信
int32 ABMPrompt::divide_sms_len(const aistring &strSmsContent,
                                const int32 iDivideLen,
                                vector<aistring> *out_strList)
{
    if (strSmsContent.length() <= iDivideLen)
    {
        out_strList->push_back(strSmsContent);
    }
    else
    {
        const char *pSmsContent = strSmsContent.c_str(); //原短信内容的数组指针
        //vector<aistring>  t_vSplitContent; //存放分割后的短信
        int iConetLen = 0; //字段长度
        int iNum = 0;      //记录字段中单字节字符的数目。
        int iBeginPos = 0;
        int iEndPos = 0;
        while (pSmsContent[iEndPos])
        {
            if ((pSmsContent[iEndPos] & 0x80) == 0x00) //字节的值小于 二进制"1000 0000"的值时，为视单字节字符
            {
                ++iNum;
            }
            ++iConetLen;
            ++iEndPos;
            if (iConetLen == iDivideLen) //当短信长度足够时，截取出来。
            {
                //为了确保最后一个字符是完整的，根据非单字节的数目来取舍最后一位字节。
                if (((iConetLen - iNum) % 2) == 0)
                {
                    aistring tmp_str = strSmsContent.substr(iBeginPos, iEndPos - iBeginPos);
                    out_strList->push_back(tmp_str);
                }
                else
                {
                    aistring tmp_str = strSmsContent.substr(iBeginPos, (--iEndPos) - iBeginPos);
                    out_strList->push_back(tmp_str);
                }
                iBeginPos = iEndPos;
                iNum = 0;
                iConetLen = 0;
            }
        }
        //剩下长度不足的内容做为最后一条短信。
        if (iBeginPos < iEndPos)
        {
            aistring tmp_str = strSmsContent.substr(iBeginPos, iEndPos - iBeginPos);
            out_strList->push_back(tmp_str);
        }
    }

    return out_strList->size();
}

// 查询abmmdb接口
int32 ABMPrompt::do_abmCommonQuery(
    const MAbmRdlCommonDef::SAbmBalanceQueryUp &sUp,
    MAbmRdlCommonDef::SAbmBalanceQueryRet &sRet,
    SOBSession *pSession)
{

    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> enter into CQueryData::query_abmMdb!");

    try
    {
        abm_mdb_serv_common::CAbmMdbServCommon cAbmMdbCommon;
        LOG_TRACE(sUp.to_string().c_str());
        ES_BEGIN_RUN_TIME("abmpropt -> query abm mdb")
        cAbmMdbCommon.Post4Queryabmtables(sUp, sRet, pSession);
        ES_END_RUN_TIME
        LOG_TRACE(sRet.to_string().c_str());
    }
    catch (err_info_service::CAIException &cSalExcp)
    {
        LOG_ERROR(ABMPROMPT_MDB_EXCEPTION, "\n==[ABM_SERV_ABMPROMPT]==>==>Post4Queryabmtables Err: sal exception, "
                                           "err code = %lld, err msg: \"%s\", log record: \"%s\"",
                  cSalExcp.get_code(),
                  cSalExcp.get_message().c_str(),
                  cSalExcp.get_logrecord().c_str());
        return ABMPROMPT_MDB_EXCEPTION;
    }
    catch (...)
    {
        LOG_ERROR(ABMPROMPT_MDB_EXCEPTION, "\n==[ABM_SERV_ABMPROMPT]==> Post4Queryabmtables system exception");
        return ABMPROMPT_MDB_EXCEPTION;
    }
    return ABMPROMPT_OK;
}

// 查询abmmdb中账户的结余，若结余大于0，则不执行停机工单
bool ABMPrompt::check_leaveFee(int64 llAcctId, int32 lNotificationId, SOBSession *pSession)
{
    //notification_id 不判断结余的情况
    AISTD string strValue;
    int32 iRetV = get_sysParamter( "ABM_NOCHK_BALANCE_RULE", strValue );
    CStringList notificationIdList;
    cdk::strings::Split(strValue, "|", notificationIdList);
    for (CStringList::iterator it = notificationIdList.begin(); it != notificationIdList.end(); ++it)
    {
        if (lNotificationId == atoi((*it).c_str()))
        {
            LOG_TRACE("\n==[check_leaveFee]==> lNotificationId = %d ", lNotificationId);
            return true;//存在notification_id 不判断结余
        }
    }
    // 查询ABMMDB中的账户结余表
    MAbmRdlCommonDef::SAbmBalanceQueryUp sQueryUp;
    MAbmRdlCommonDef::SAbmBalanceQueryRet sQueryRet;
    sQueryUp.set_tableFlag(TABLE_LEAVE_FEE);
    sQueryUp.set_acctId(llAcctId);
    LOG_TRACE("\n==[query_balance]==> recv acct_id = %lld ", llAcctId);
    int32 iRet;
    ES_BEGIN_RUN_TIME("abmprompt -> do_abmCommonQuery")
    iRet = do_abmCommonQuery(sQueryUp, sQueryRet, pSession);
    ES_END_RUN_TIME

    if ((iRet == ABMPROMPT_OK) && (!sQueryRet.acctLeaveList().empty()))
    {
        // 从查询结果中获取结余值
        MAbmRdlCommonDef::SAcctLeaveFeeList &listAcctLeaveFee = sQueryRet.acctLeaveList();
        int64 llLeaveFee = listAcctLeaveFee.begin()->get_creditBalance();

        LOG_TRACE("\n====[ABM_QUERY_TABLE_LEAVE_FEE]==> the acct_id(%lld)'s leavefee is %lld..",
                  llAcctId, llLeaveFee);
        if (llLeaveFee > 0)
        {
            LOG_ERROR(-1, "\n====[ABM_QUERY_TABLE_LEAVE_FEE]==> the acct_id(%lld)'s leavefee is %lld. not need down.",
                      llAcctId, llLeaveFee);
            return false; // 结余大于0，停机工单不执行
        }
    }
    LOG_TRACE("\n====[ABM_QUERY_TABLE_LEAVE_FEE]==> the acct_id(%lld)'s leavefee less 0， or not get leavefee from abmmdb", llAcctId);
    return true; // 停机工单正常处理
}

int32 ABMPrompt::getExtContent(int32 &iRegionCode, int64 &llResourceId, int64 &llTemplateId, AISTD string &strContent, AISTD string &strTempFlag)
{
    int32 iRet = SDL_OK;
    try
    {
        AISTD string strTemplateFlag;
        int64 llExtTemplateId = 0;
        MAbmInterfacePromptDb::CTfFUserremindOntimeList listTfFUserremindOntime;
        listTfFUserremindOntime.clear();
        xc::CSnapshot cSnap("ZW::ABM_BALANCE");
        xc::CQueryHolder<ZW::ABM_BALANCE::CNotifydealSysNotificationTemplate::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotifydealSysNotificationTemplate::GetContainerName());
        ZW::ABM_BALANCE::CNotifydealSysNotificationTemplate::Type::iterator iter = cQueryHolder.GetContainer().find(llTemplateId);

        if (iter != cQueryHolder.GetContainer().end())
        {
            strTemplateFlag = iter->second.GetTemplateFlag();
            strTempFlag = strTemplateFlag;
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> strTemplateFlag=%s", strTemplateFlag.c_str());
            if (strTemplateFlag.size() > 0)
            {
                AISTD string::size_type pos;
                pos = strTemplateFlag.find(REMIND_TEMPLATE);
                if (pos != AISTD string::npos)
                {
                    llExtTemplateId = atoi((strTemplateFlag.substr(sizeof(REMIND_TEMPLATE) - 1, sizeof(strTemplateFlag) - 1)).c_str());
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> has found llExtTemplateId = %lld", llExtTemplateId);
                    if (llExtTemplateId > 0 && llResourceId > 0)
                    {
                        ob_whereCond condition;
                        ob_kernel<MAbmInterfacePromptDb::CTfFUserremindOntimeList> query("ZG"); //按地市分用户
                        query.RemovePartitionKey("REGION_CODE");
                        query.AddPartitionKey("REGION_CODE", cdk::strings::Itoa(iRegionCode));
                        condition << OCS(" USER_ID = ") << llResourceId;
                        condition << OCS(" AND TEMPLETE_ID = ") << llExtTemplateId;
                        query.select_allExt(listTfFUserremindOntime, condition, NULL, false);
                        if (!listTfFUserremindOntime.empty())
                        {
                            strContent = listTfFUserremindOntime[0].get_context();
                            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> the ExtContent : %s", strContent.c_str());
                        }
                        else
                        {
                            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> cannot find record of resource_id = %lld and llExtTemplateId = %lld ...", llResourceId, llExtTemplateId);
                        }
                    }
                    else
                    {
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==>  llExtTemplateId < 0 or llResourceId < 0 , nothing to deal ...");
                    }
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> TemplateFlag is not like '%s' , nothing to deal ... ", REMIND_TEMPLATE);
                }
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> TemplateFlag is null , nothing to deal ...");
            }
        }
        else
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> can not find any record of templateId = %lld ,check table SYS_NOTIFICATION_TEMPLATE", llTemplateId);
            return SDL_FAIL;
        }
    }
    catch (XC_EXCEPTION &e)
    {
        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==>  Code: %lld, message: %s, logrecord: %s",
                  e.get_code(), e.get_message().c_str(), e.get_logrecord().c_str());

        throw;
    }
    return iRet;
}

AISTD string ABMPrompt::ltoa(int64 l)
{
    char buf[255];
    sprintf(buf, "%lld", l);
    AISTD string ret = (const char *)buf;
    return ret;
}

int32 ABMPrompt::insertOneToBeyondPackage(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    MAbmInterfacePromptDb::CIuserBeyondPackage cIUserBeyondPacage;
    MAbmInterfacePromptDb::CIuserNearLimit cIUserNearLimit;

    int32 iRetValue = ABMPROMPT_OK;

    vector<string> vecString1;
    vector<string> vecString2;
    vector<string> vecString3;
    AISTD string strProcess = m_imsNtfIvr.get_extend1();

    split(":", strProcess.c_str(), false, vecString1);

    if (vecString1.size() == 2)
    {
        if ("0" == vecString1[0])
        { //流量近饱和用户 extend1字段格式为"0:TOTAL_RES,1024;USED_RES,512;LEFT_RES,512;PERSCENTAGE_TRIGGERED,0.8;" 表字段不区分顺序
            split(";", vecString1[1].c_str(), true, vecString2);

            if (vecString2.size() == 4)
            {
                for (unsigned int iIndex = 0; iIndex < vecString2.size(); iIndex++)
                {
                    split(",", vecString2[iIndex].c_str(), false, vecString3);
                    if (vecString3.size() == 2)
                    {
                        if (vecString3[0] == "TOTAL_RES")
                        {
                            cIUserNearLimit.set_totalRes(cdk::strings::Atol64(vecString3[1]));
                        }
                        else if (vecString3[0] == "USED_RES")
                        {
                            cIUserNearLimit.set_usedRes(cdk::strings::Atol64(vecString3[1]));
                        }
                        else if (vecString3[0] == "LEFT_RES")
                        {
                            cIUserNearLimit.set_leftRes(cdk::strings::Atol64(vecString3[1]));
                        }
                        else if (vecString3[0] == "PERSCENTAGE_TRIGGERED")
                        {
                            cIUserNearLimit.set_perscentageTriggered(atof(vecString3[1].c_str()));
                        }
                    }
                    else
                    {
                        LOG_ERROR(ABMPROMPT_ERROR_DATA_FORMAT, "\n==[ABM_SERV_ABMPROMPT]==> cotent parase error!========");
                        iRetValue = ABMPROMPT_ERROR;
                    }
                }

                imsNtfBeyondPackageCommon_to_cIUser(cIUserNearLimit, m_imsNtfIvr);
                m_listIUserNearLimit.clear();
                m_listIUserNearLimit.push_back(cIUserNearLimit);
                if (ABMPROMPT_OK == iRetValue)
                {
                    iRetValue = insert_DataToUserNearLimit(pSession, cErrorMsg);
                }
            }
            else
            {
                iRetValue = ABMPROMPT_ERROR_DATA_FORMAT;
            }
        }
        else if ("1" == vecString1[0])
        { //超套用户 extend1字段格式为"0:OVER_BALANCE,200;OVER_RES,512;TOTAL_RES,512;USED_RES,1024;" 表字段不区分顺序
            split(";", vecString1[1].c_str(), true, vecString2);

            if (vecString2.size() == 4)
            {
                for (unsigned int iIndex = 0; iIndex < vecString2.size(); iIndex++)
                {
                    split(",", vecString2[iIndex].c_str(), false, vecString3);
                    if (vecString3.size() == 2)
                    {
                        if (vecString3[0] == "OVER_BALANCE")
                        {
                            cIUserBeyondPacage.set_overBalance(cdk::strings::Atol64(vecString3[1]));
                        }
                        else if (vecString3[0] == "OVER_RES")
                        {
                            cIUserBeyondPacage.set_overRes(cdk::strings::Atol64(vecString3[1]));
                        }
                        else if (vecString3[0] == "TOTAL_RES")
                        {
                            cIUserBeyondPacage.set_totalRes(cdk::strings::Atol64(vecString3[1]));
                        }
                        else if (vecString3[0] == "USED_RES")
                        {
                            cIUserBeyondPacage.set_usedRes(cdk::strings::Atol64(vecString3[1]));
                        }
                    }
                    else
                    {
                        LOG_ERROR(ABMPROMPT_ERROR_DATA_FORMAT, "\n==[ABM_SERV_ABMPROMPT]==> cotent parase error!========");
                        iRetValue = ABMPROMPT_ERROR;
                    }
                }
                imsNtfBeyondPackageCommon_to_cIUser(cIUserBeyondPacage, m_imsNtfIvr);
                m_listIUserBeyondPackage.clear();
                m_listIUserBeyondPackage.push_back(cIUserBeyondPacage);
                if (ABMPROMPT_OK == iRetValue)
                {
                    iRetValue = insert_DataToUserBeyondPackage(pSession, cErrorMsg);
                }
            }
            else
            {
                LOG_ERROR(ABMPROMPT_ERROR_DATA_FORMAT, "\n==[ABM_SERV_ABMPROMPT]==> cotent parase error!========");
                iRetValue = ABMPROMPT_ERROR;
            }
        }
        else
        { //error
            LOG_ERROR(ABMPROMPT_ERROR_DATA_FORMAT, "\n==[ABM_SERV_ABMPROMPT]==> cotent parase error!========");
            iRetValue = ABMPROMPT_ERROR;
        }
    }
    else
    { //error
        LOG_ERROR(ABMPROMPT_ERROR_DATA_FORMAT, "\n==[ABM_SERV_ABMPROMPT]==> cotent parase error!========");
        iRetValue = ABMPROMPT_ERROR;
    }
    return iRetValue;
}

/* 流量近饱和用户数据表 */
int32 ABMPrompt::insert_DataToUserNearLimit(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    if (m_listIUserNearLimit.empty())
    {
        return ABMPROMPT_OK;
    }
    TRY_BEGIN
    ob_kernel<MAbmInterfacePromptDb::CIuserNearLimitList> serviceInsert("JD");

    serviceInsert.AddPartitionKey("SO_DATE", m_listIUserNearLimit[0].get_updateTime().toString("%Y%M").c_str());

    serviceInsert.insert(m_listIUserNearLimit, false);

    CATCH_END_NOTHROW

    return ABMPROMPT_OK;
}

/* 超套用户数据表 */
int32 ABMPrompt::insert_DataToUserBeyondPackage(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    if (m_listIUserBeyondPackage.empty())
    {
        return ABMPROMPT_OK;
    }
    TRY_BEGIN
    ob_kernel<MAbmInterfacePromptDb::CIuserBeyondPackageList> serviceInsert("JD");

    serviceInsert.AddPartitionKey("SO_DATE", m_listIUserBeyondPackage[0].get_updateTime().toString("%Y%M").c_str());

    serviceInsert.insert(m_listIUserBeyondPackage, false);

    CATCH_END_NOTHROW

    return ABMPROMPT_OK;
}

int32 ABMPrompt::getLocalIp(AISTD string &localIp)
{
    char name[256];
    gethostname(name, sizeof(name));

    struct hostent *host = gethostbyname(name);
    char ipStr[32];
    const char *ret = inet_ntop(host->h_addrtype, host->h_addr_list[0], ipStr, sizeof(ipStr));
    if (NULL == ret)
    {
        return ABMPROMPT_ERROR;
    }
    localIp = ipStr;
    return ABMPROMPT_OK;
}

int32 ABMPrompt::getSourceDbFromRegionCode(AISTD string strRegionCode, AISTD string &strSourceDb)
{
    aistring strValue;
    int32 iRet = ABMPROMPT_OK;
    iRet = get_sysParamter("ABM_PROMPT_REGIONCODE_SOURCEDB", strValue);
    if (ABMPROMPT_OK != iRet)
    {
        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> get source db fail! use default act42");
        strSourceDb = "act42";
    }
    else
    {
        int i = 0;
        vector<aistring> strOneValue;
        strOneValue.clear();
        cdk::strings::Split(strValue, "|", strOneValue);
        // 0731:uop_act42|0732:uop_act42|0735:uop_act41
        if (strOneValue.size() <= 0)
        {
            LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> get onevalue fail! use default act42");
            strSourceDb = "act42";
            return ABMPROMPT_XC_ERROR;
        }
        for (i = 0; i < strOneValue.size(); i++)
        {
            vector<aistring> sourceString;
            sourceString.clear();
            cdk::strings::Split(strOneValue[i], ":", sourceString);
            if (sourceString.size() != 2)
            {
                LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> get source db fail! use default act42");
                strSourceDb = "act42";
                return ABMPROMPT_XC_ERROR;
            }
            if (sourceString[0] == strRegionCode)
            {
                strSourceDb = sourceString[1];
                break;
            }
        }
        if (i >= strOneValue.size())
        {
            LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> sysparam error,regionCode[%s] is not has! use default act42", strRegionCode.c_str());
            strSourceDb = "act42";
            return ABMPROMPT_XC_ERROR;
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> RegionCode:%s sourceDb:%s", strRegionCode.c_str(), strSourceDb.c_str());
    return iRet;
}

int32 ABMPrompt::getSmsKindCode(AISTD string strSrcSmsKindCode, AISTD string &strTarSmsKindCode)
{
    if (strSrcSmsKindCode.empty())
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> strSrcSmsKindCode is empty, return dafault value 08");
        strTarSmsKindCode = "08";
        return ABMPROMPT_OK;
    }
    if (("73" == strSrcSmsKindCode) || ("59" == strSrcSmsKindCode) || ("68" == strSrcSmsKindCode) || ("60" == strSrcSmsKindCode) || ("71" == strSrcSmsKindCode) || ("64" == strSrcSmsKindCode) || ("61" == strSrcSmsKindCode) || ("62" == strSrcSmsKindCode) || ("51" == strSrcSmsKindCode) || ("54" == strSrcSmsKindCode) || ("75" == strSrcSmsKindCode) || ("65" == strSrcSmsKindCode))
    {
        strTarSmsKindCode = "06";
    }
    else if (("69" == strSrcSmsKindCode) || ("72" == strSrcSmsKindCode) || ("74" == strSrcSmsKindCode) || ("52" == strSrcSmsKindCode) || ("93" == strSrcSmsKindCode) || ("76" == strSrcSmsKindCode) || ("84" == strSrcSmsKindCode))
    {
        strTarSmsKindCode = "07";
    }
    else if (("02" == strSrcSmsKindCode) || ("3" == strSrcSmsKindCode))
    {
        strTarSmsKindCode = "14";
    }
    else
    {
        strTarSmsKindCode = "08";
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> strSrcSmsKindCode[%s], strTarSmsKindCode[%s]", strSrcSmsKindCode.c_str(), strTarSmsKindCode.c_str());
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_wechat(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_wechat->query_data")
    query_data(m_listImsNtfWechat);
    ES_END_RUN_TIME

    if (m_listImsNtfWechat.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfWechat is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_wechat order : %d orders from table %s.", m_listImsNtfWechat.size(), m_sourceTable.c_str());

    m_listImsNtfWechatHis.clear();
    int32 iCount = 0;
    //m_listStatIn.clear();
    //m_listStatUp.clear();
    for (m_itrImsNtfWechat = m_listImsNtfWechat.begin(); m_itrImsNtfWechat != m_listImsNtfWechat.end(); m_itrImsNtfWechat++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_wechat->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfWechat = *m_itrImsNtfWechat;
        m_actionId = m_imsNtfWechat.get_actionId();

        int32 iRetValue = ABMPROMPT_OK;
        if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
        {
            AISTD string strErrMsg("action_id = ");
            strErrMsg.append(cdk::strings::Itoa(m_actionId));
            strErrMsg.append("  get action_id info failed");
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
            cErrorMsg.set_hint(strErrMsg);
            iRetValue = ABMPROMPT_ERROR;
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
        }
        if (m_imsNtfWechat.get_notifContent().empty())
        {
            iRetValue = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
            cErrorMsg.set_hint("notif_content is empty");
        }
        else if (!check_phoneId(m_imsNtfWechat.get_phoneId()))
        {
            iRetValue = ABMPROMPT_ORDER_PHONE;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ORDER_PHONE));
            cErrorMsg.set_hint("phone_id is inaccurate ,don't need to send message");
        }
        else if (!check_notifyContent(m_imsNtfWechat.get_notifContent()))
        {
            iRetValue = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ERROR_MESSAGE));
            cErrorMsg.set_hint("the notif_content contains  '<' or '>' ,some param_id don't have be replaced.");
        }

        if (ABMPROMPT_OK == iRetValue)
        {
            //飞书必达平台对接改造 qiankun3 20190722
            m_nActionLevel = m_imsNtfWechat.get_actionLevel();
            if (ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel) //飞书必达微信提醒工单
            {
                //准备数据
                m_listTiOFsbdWxmsg.clear();

                AISTD string strQueueId = ltoa(m_imsNtfWechat.get_queueId());
                //注意：当工单量每秒超过百万条时，这种batchId拼接方式会出现重复
                AISTD string strAppBatchId = "ACT" + m_dtDateTime.toString("%Y%M%D%H%N%S") + strQueueId.substr(strQueueId.length() - 6); //账务为ACT+YYYYMMDDHHmmSS+6位序列号
                int32 iPartitionId = atoi(strAppBatchId.substr(strAppBatchId.length() - 3).c_str());

                AISTD string strTemplateId = "";
                AISTD string strMsgType = "";
                AISTD string strTemplateUrl = "";
                AISTD string strTemplateContent = "";
                int32 iMonth = 0;
                int32 iDay = 0;
                int64 llResourceId = m_imsNtfWechat.get_resourceId();
                AISTD string strRecvObject = m_imsNtfWechat.get_phoneId();
                char eparchyCode[5];
                sprintf(eparchyCode, "%04d", m_imsNtfWechat.get_regionCode());

                iMonth = atoi(m_dtDateTime.toString("%M").c_str());
                iDay = atoi(m_dtDateTime.toString("%D").c_str());

                //字段数量不足，提醒内容拼接在notifContent字段种
                LOG_TRACE("[WXMSG]=======>get_notifContent:%s", m_imsNtfCredit.get_notifContent().c_str());
                CStringList lstStrWithMsgType;
                CStringList lstWechatStr;
                int32 iMsgTypeSplitRet = split_msgType_notifyContent(m_imsNtfCredit.get_notifContent(), "|||", lstStrWithMsgType);
                if (iMsgTypeSplitRet == 2)
                {
                    strMsgType = lstStrWithMsgType.at(1);
                    base_divide_str(lstStrWithMsgType.at(0), "|", lstWechatStr);
                    if (lstWechatStr.size() == 2)
                    {
                        strTemplateContent = lstWechatStr.at(0);
                        strTemplateId = lstWechatStr.at(1);
                    }

                    if (lstWechatStr.size() == 3)
                    {
                        strTemplateContent = lstWechatStr.at(0);
                        strTemplateId = lstWechatStr.at(1);
                        strTemplateUrl = lstWechatStr.at(2);
                    }
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_imsNtfCredit.get_notifContent error");
                }

                MAbmInterfacePromptDb::CTiOFsbdWxmsg tiOFsbdWxmsg;
                tiOFsbdWxmsg.set_appBatchId(strAppBatchId);
                tiOFsbdWxmsg.set_partitionId(iPartitionId);
                tiOFsbdWxmsg.set_recvId(llResourceId);
                tiOFsbdWxmsg.set_recvObject(strRecvObject);
                tiOFsbdWxmsg.set_templateId(strTemplateId);
                tiOFsbdWxmsg.set_msgParams(strTemplateContent);
                tiOFsbdWxmsg.set_url(strTemplateUrl);
                tiOFsbdWxmsg.set_msgType(strMsgType);
                tiOFsbdWxmsg.set_sendType(0);
                tiOFsbdWxmsg.set_pushType(0);
                tiOFsbdWxmsg.set_pushLevel(1);
                tiOFsbdWxmsg.set_eparchyCode(eparchyCode);
                tiOFsbdWxmsg.set_referTime(m_dtDateTime);
                tiOFsbdWxmsg.set_dealTime(m_dtDateTime);
                tiOFsbdWxmsg.set_dealState(0);
                tiOFsbdWxmsg.set_month(iMonth);
                tiOFsbdWxmsg.set_day(iDay);

                m_listTiOFsbdWxmsg.push_back(tiOFsbdWxmsg);

                //入表
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_remind->insertTiOFsbdWxmsg")
                iRetValue = insertTiOFsbdWxmsg(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            else
            {
                m_listTiOSmsWechat.Clear();
                MAbmInterfacePromptDb::CTiOSmsWechat tiOSmsTmp;
                CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
                char eparchyCode[5];
                sprintf(eparchyCode, "%04d", m_imsNtfWechat.get_regionCode());
                char recv4[17];
                sprintf(recv4, "%d", m_imsNtfWechat.get_amount());

                tiOSmsTmp.set_smsNoticeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfWechat.get_queueId());
                tiOSmsTmp.set_eparchyCode(eparchyCode);
                tiOSmsTmp.set_inModeCode("0");
                tiOSmsTmp.set_smsChannelCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
                tiOSmsTmp.set_sendObjectCode(2);
                tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                tiOSmsTmp.set_sendCountCode(1);
                tiOSmsTmp.set_recvObjectType("00");
                tiOSmsTmp.set_recvObject(m_imsNtfWechat.get_phoneId());
                tiOSmsTmp.set_id(m_imsNtfWechat.get_resourceId());
                tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                tiOSmsTmp.set_smsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                tiOSmsTmp.set_noticeContentType("0");
                tiOSmsTmp.set_noticeContent(m_imsNtfWechat.get_notifContent());
                //tiOSmsTmp.set_referedCount(const int32 & value);
                tiOSmsTmp.set_forceReferCount(1);
                //tiOSmsTmp.set_forceObject(const aistring & value);
                //tiOSmsTmp.set_forceStartTime(const CBSDateTime & value);
                //tiOSmsTmp.set_forceEndTime(const CBSDateTime & value);
                tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                tiOSmsTmp.set_referTime(m_dtDateTime);
                tiOSmsTmp.set_referStaffId("CREDIT00");
                tiOSmsTmp.set_referDepartId("CREDI");
                tiOSmsTmp.set_dealTime(dealTime);
                //tiOSmsTmp.set_dealStaffid(const aistring & value);
                //tiOSmsTmp.set_dealDepartid(const aistring & value);
                tiOSmsTmp.set_dealState("0");
                //tiOSmsTmp.set_remark(const aistring & value);
                //tiOSmsTmp.set_revc1(const aistring & value);
                //tiOSmsTmp.set_revc2(const aistring & value);
                //tiOSmsTmp.set_revc3(const aistring & value);
                tiOSmsTmp.set_revc4(recv4); //结余值
                tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));

                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms_wechat  data : eparchy_code= %s,queue_id=%lld,acct_id = %lld",
                          tiOSmsTmp.get_eparchyCode().c_str(),
                          m_imsNtfWechat.get_queueId(),
                          m_imsNtfWechat.get_acctId());

                m_listTiOSmsWechat.push_back(tiOSmsTmp);
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_wechat->insert_to_sms_wechat")
                iRetValue = insertIntoSmsWechat(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
        }

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_wechat->delete_inserthis")
        if (iRetValue != ABMPROMPT_OK && ABMPROMPT_ORDER_PHONE != iRetValue)
        {
            //contrl_rollback(pSession, cErrorMsg);
            m_imsNtfWechat.set_status(ABM_PROMPT_PROCESS_FAILED);
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfWechat.set_remark(strRemark.substr(0, 1024));
            m_imsNtfWechat.set_soDate(m_dtDateTime);
            update_data<MAbmInterfacePromptDb::CImsNtfWechatList>(pSession, cErrorMsg, m_imsNtfWechat);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Wechat sub table, %s : %s return to ims_ntf_wechat",
                      m_imsNtfWechat.get_queueId(),
                      m_imsNtfWechat.get_acctId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfWechatHis imsNtfWechatHis;
            transToHis(m_imsNtfWechat, imsNtfWechatHis);
            if (iRetValue == ABMPROMPT_OK && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel)
            {
                imsNtfWechatHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                imsNtfWechatHis.set_remark("insert into ti_o_sms_wechat OK");
            }
            else if (iRetValue == ABMPROMPT_OK && ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel)
            {
                imsNtfWechatHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                imsNtfWechatHis.set_remark("insert into ti_o_fsbd_wxmsg OK");
            }
            else
            {
                imsNtfWechatHis.set_status(ABM_PROMPT_PROCESS_PHONE_FILTER);
                imsNtfWechatHis.set_remark((cErrorMsg.get_errorCode() + ":" + cErrorMsg.get_hint()).substr(0, 2048));
            }

            imsNtfWechatHis.set_soDate(m_dtDateTime);
            m_listImsNtfWechatHis.push_back(imsNtfWechatHis);

            iRetValue = insert_dataToHisWechat(pSession, cErrorMsg);
            m_listImsNtfWechatHis.clear();

            if (ABMPROMPT_OK == iRetValue)
            {
                iRetValue = delete_data<MAbmInterfacePromptDb::CImsNtfWechatList>(cErrorMsg, m_imsNtfWechat);
                if (iRetValue != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    //m_listStatUp.clear();
                    //m_listStatIn.clear();
                    m_imsNtfWechat.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfWechat.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfWechat.set_soDate(m_dtDateTime);
                    update_data<MAbmInterfacePromptDb::CImsNtfWechatList>(pSession, cErrorMsg, m_imsNtfWechat);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Wechat sub table, %s : %s return to ims_ntf_wechat",
                              m_imsNtfWechat.get_queueId(),
                              m_imsNtfWechat.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_wechat->statImsNtf")
                    statImsNtf(pSession, cErrorMsg, m_sysNotificationActionExtsmsIter->second.GetTradeTypeCode(), m_imsNtfWechat);
                    ES_END_RUN_TIME
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Wechat sub_table is done successed",
                              m_imsNtfWechat.get_queueId(),
                              m_imsNtfWechat.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                }
            }
            else
            {
                contrl_rollback(pSession, cErrorMsg);
                //m_listStatIn.clear();
                //m_listStatUp.clear();
                m_imsNtfWechat.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfWechat.set_remark(strRemark.substr(0, 1024));
                m_imsNtfWechat.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfWechatList>(pSession, cErrorMsg, m_imsNtfWechat);
                contrl_commit(pSession, cErrorMsg);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Wechat sub table, %s : %s return to ims_ntf_wechat",
                          m_imsNtfWechat.get_queueId(),
                          m_imsNtfWechat.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
            }
        }

        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            //upStatPrompt(pSession, cErrorMsg);
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME

        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    //upStatPrompt(pSession, cErrorMsg);
    contrl_commit(pSession, cErrorMsg);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> loop process success : %d orders processed. ", m_listImsNtfWechat.size());
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

//Added by zhangwm3 for repair sms context in 20210111 begin
bool ABMPrompt::check_content(const AISTD string & r_strText,const AISTD string &r_strSub, int &r_iFirstPos,int &r_iLastPos)
{
    if ( r_strText.empty() || r_strSub.empty())
    {
        return false;
    }
    
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> check_context str = %s", r_strText.c_str() );
    
    r_iFirstPos = r_strText.find(r_strSub);
    r_iLastPos  = r_strText.rfind(r_strSub);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> FirstPos = %d,LastPos = %d", r_iFirstPos,r_iLastPos);
    
    if( r_iLastPos <=  r_iFirstPos )
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> no have %s ",r_strSub.c_str());
        return false;
    }

    return true;
}

bool ABMPrompt::repair_context_data(AISTD string &r_strContext)
{
    int t_iFirstPos = -1;
    int t_iLastPos  = -1;
    if (!check_content(r_strContext,"|",t_iFirstPos,t_iLastPos))
    {
        return false;
    }

    //将元截取掉,默认2个字符宽度
    aistring strtmpValue("2"); 
    get_sysParamter("SMS_CUT_LENGTH", strtmpValue);
    unsigned int t_iSmsCutLen = atoi(strtmpValue.c_str());

    //获取最后的分隔符
    AISTD string strFg = r_strContext.substr( t_iLastPos, 1 );
    
    r_strContext = r_strContext.replace(t_iLastPos, 1, "");

    r_strContext = r_strContext.insert(t_iLastPos-t_iSmsCutLen, strFg);
    
    LOG_TRACE( "\n==[ABM_SERV_ABMPROMPT]==> strData = %s,SmsCutLen = %d", r_strContext.c_str(),t_iSmsCutLen );
    return true;
}
//Added by zhangwm3 for repair sms context in 20210111 end

//add by xupp3 for yunnanV8 begin 云南短信合并功能
bool ABMPrompt::get_context_data( AISTD string& strContext, double& llReTV, AISTD string& strContextDesc, bool bReplace, double& dRepVal )
{
    int t_iFirstPos = -1;
    int t_iLastPos  = -1;
    if ( !check_content(strContext,"|",t_iFirstPos,t_iLastPos) )
    {
        return false;
    }
        
    int t_iCount = t_iLastPos - t_iFirstPos - 1;
    AISTD string strData = strContext.substr( t_iFirstPos+1 , t_iCount );
    LOG_TRACE( "\n==[ABM_SERV_ABMPROMPT]==> strData = %s,Count = %d", strData.c_str(),t_iCount );

    size_t bpos = strData.find("|");
    if (bpos != std::string::npos)
    {
        strData.replace(bpos, 1, "");
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> strData = %s", strData.c_str());
    }
    llReTV = atof(strData.c_str());
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> llReTV = %.2lf", llReTV);
    if( bReplace )
    {
        int len = t_iLastPos - t_iFirstPos + 1;
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> len = %d", len);
        AISTD string strRepVal = _ftoa(dRepVal);
        strContextDesc = strContext.replace(t_iFirstPos, len, strRepVal );
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> strContextDesc = %s", strContextDesc.c_str());
    }
    return true;
}
bool ABMPrompt::isToday( int64 llCreateDate )
{
    m_dtDateTime = CBSDateTime::currentDateTime();
    int64 llTodayS = atol64(m_dtDateTime.toString("%Y%M%D%H%N%S"));
    int64 llTodayD = llTodayS/1000000;
    if(llCreateDate == llTodayD)
        return true;
    return false;
}
int32 ABMPrompt::process_mergesms(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    int32 iRetValue = ABMPROMPT_OK;

    while ( true )
    {
        m_listImsNtfGrpcredit.clear();
        m_listImsNtfMergeCredit.clear();
        m_listImsNtfMergeCreditHis.clear();

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergesms->query_data")
        query_data(m_listImsNtfMergeCredit);
        ES_END_RUN_TIME
        if (m_listImsNtfMergeCredit.empty())
        {
            if (m_pCfg->m_cfgCommon.m_nSleep > 0)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeCredit is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
                sleep(m_pCfg->m_cfgCommon.m_nSleep);
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeCredit is empty, not need sleep");
            }
            break;
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms order : %lld orders from table %s.", m_listImsNtfMergeCredit.size(), m_sourceTable.c_str());
        if( !m_listImsNtfMergeCredit.empty() )
        {
            
            string strExtend9 = m_listImsNtfMergeCredit[0].get_extend9();
            m_listImsNtfMergeCredit.clear();
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms strExtend9 = %s.", strExtend9.c_str());
            query_mergedata( m_listImsNtfMergeCredit, strExtend9 );
            if (m_listImsNtfMergeCredit.empty())
            {
                continue;
            }
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms order : %lld orders from table %s.", m_listImsNtfMergeCredit.size(), m_sourceTable.c_str());
        }
        
        CMergecreditMap mapCMergeCredit;
        for (MAbmInterfacePromptDb::CImsNtfMergecreditList::const_iterator itrImsNtfMerge = m_listImsNtfMergeCredit.begin(); itrImsNtfMerge != m_listImsNtfMergeCredit.end(); ++itrImsNtfMerge)
        {
            MAbmInterfacePromptDb::CImsNtfMergecredit cimsNtfMergeCreditT = *itrImsNtfMerge;
            LOG_TRACE((cimsNtfMergeCreditT).to_string().c_str());
            int64 llCreateDate = atol64(cimsNtfMergeCreditT.get_createDate().toString("%Y%M%D%H%N%S"));
            int64 llCreateD = llCreateDate/1000000;
            //if(!isToday( llCreateD ) )
            //    continue;
            CMergeCreditKey cMergeCreditKey( cimsNtfMergeCreditT, llCreateD );
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> key acct_id = %lld, cust_id = %lld,create_dt = %lld,phone_id = %s,product_id= %lld", 
                cMergeCreditKey.acct_id, cMergeCreditKey.cust_id,cMergeCreditKey.create_dt,cMergeCreditKey.phone_id.c_str(),cMergeCreditKey.product_id);
        
            typename CMergecreditMap::iterator iterCMergeCredit = mapCMergeCredit.find( cMergeCreditKey );
            if( iterCMergeCredit != mapCMergeCredit.end() )
            {
                iterCMergeCredit->second.push_back(cimsNtfMergeCreditT);
            }
            else
            {
                MAbmInterfacePromptDb::CImsNtfMergecreditList listCImsNtfMergecredit;
                listCImsNtfMergecredit.push_back(cimsNtfMergeCreditT);
                mapCMergeCredit.insert(AISTD make_pair( cMergeCreditKey, listCImsNtfMergecredit ) );
            }
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms order mapCMergeCredit.size(): %lld ", mapCMergeCredit.size());
        
        for ( CMergecreditMap::iterator iterMgMp = mapCMergeCredit.begin(); iterMgMp != mapCMergeCredit.end(); ++iterMgMp )
        {
            MAbmInterfacePromptDb::CImsNtfMergecreditList &m_listImsNtfMergeCredit = iterMgMp->second;
            MAbmInterfacePromptDb::CImsNtfMergecreditHisList MergeDateHisList;
            MAbmInterfacePromptDb::CImsNtfGrpcreditList  GrpDateList;
            mergesms(pSession, m_listImsNtfMergeCredit, MergeDateHisList,GrpDateList, cErrorMsg);
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeCredit.size() = %lld.", m_listImsNtfMergeCredit.size() );
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> MergeDateHisList.size() = %lld.", MergeDateHisList.size() );
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> GrpDateList() = %lld.",  GrpDateList.size() );
            doMergeSmsToDB(m_listImsNtfMergeCredit,MergeDateHisList ,GrpDateList,true, pSession, cErrorMsg);
        }
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}


int32 ABMPrompt::process_mergesmsStop(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    int32 iRetValue = ABMPROMPT_OK;
    while ( true )
    {
        m_listImsNtfMergeStop.clear();
        m_listImsNtfMergeCreditHis.clear();
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergesms->query_data")
        query_data(m_listImsNtfMergeStop);
        ES_END_RUN_TIME
        if (m_listImsNtfMergeStop.empty())
        {
            if (m_pCfg->m_cfgCommon.m_nSleep > 0)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> process_mergesmsStop is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
                sleep(m_pCfg->m_cfgCommon.m_nSleep);
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> process_mergesmsStop is empty, not need sleep");
            }
            break;
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesmsStop order : %d orders from table %s.", m_listImsNtfMergeStop.size(), m_sourceTable.c_str());
        if( !m_listImsNtfMergeStop.empty() )
        {
            
            string strExtend9 = m_listImsNtfMergeStop[0].get_extend9();
            m_listImsNtfMergeStop.clear();
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms strExtend9 = %s.", strExtend9.c_str());
            query_mergedata( m_listImsNtfMergeStop, strExtend9 );
            if (m_listImsNtfMergeStop.empty())
            {
                continue;
            }
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms order : %lld orders from table %s.", m_listImsNtfMergeStop.size(), m_sourceTable.c_str());
        }

        CMergestopMap mapCMergeStop;
        for (MAbmInterfacePromptDb::CImsNtfMergestopList::const_iterator itrImsNtfMerge = m_listImsNtfMergeStop.begin(); itrImsNtfMerge != m_listImsNtfMergeStop.end(); ++itrImsNtfMerge)
        {
            MAbmInterfacePromptDb::CImsNtfMergestop cimsNtfMergeStopT = *itrImsNtfMerge;
            LOG_TRACE((cimsNtfMergeStopT).to_string().c_str());
            int64 llCreateDate = atol64(cimsNtfMergeStopT.get_createDate().toString("%Y%M%D%H%N%S"));
            int64 llCreateD = llCreateDate/1000000;
            //if(!isToday( llCreateD ) )
            //    continue;
            CMergeCreditKey cMergeCreditKey( cimsNtfMergeStopT, llCreateD );
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> key acct_id = %lld, cust_id = %lld,create_dt = %lld,phone_id = %s,product_id= %lld", 
                cMergeCreditKey.acct_id, cMergeCreditKey.cust_id,cMergeCreditKey.create_dt,cMergeCreditKey.phone_id.c_str(),cMergeCreditKey.product_id);
        
            CMergestopMap::iterator iterCMergeStop = mapCMergeStop.find( cMergeCreditKey );
            if( iterCMergeStop != mapCMergeStop.end() )
            {
                iterCMergeStop->second.push_back(cimsNtfMergeStopT);
            }
            else
            {
                MAbmInterfacePromptDb::CImsNtfMergestopList listCImsNtfMergestop;
                listCImsNtfMergestop.push_back(cimsNtfMergeStopT);
                mapCMergeStop.insert(AISTD make_pair( cMergeCreditKey, listCImsNtfMergestop ) );
            }
        }

        for ( CMergestopMap::iterator iterMgMp = mapCMergeStop.begin(); iterMgMp != mapCMergeStop.end(); ++iterMgMp )
        {
            MAbmInterfacePromptDb::CImsNtfMergestopList &m_listImsNtfMergestop = iterMgMp->second;
            MAbmInterfacePromptDb::CImsNtfMergestopHisList MergeDateHisList;
            MAbmInterfacePromptDb::CImsNtfGrpcreditList  GrpDateList;
            mergesms(pSession, m_listImsNtfMergestop, MergeDateHisList,GrpDateList, cErrorMsg);
            doMergeSmsToDB(m_listImsNtfMergestop,MergeDateHisList ,GrpDateList,true, pSession, cErrorMsg);
        }
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}


int32 ABMPrompt::process_mergesmsWarn(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    int32 iRetValue = ABMPROMPT_OK;
    while ( true )
    {
        m_listImsNtfMergeWarn.clear();
        m_listImsNtfMergeWarnHis.clear();
        
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergesmsWarn->query_data")
        query_data(m_listImsNtfMergeWarn);
        ES_END_RUN_TIME
        if (m_listImsNtfMergeWarn.empty())
        {
            if (m_pCfg->m_cfgCommon.m_nSleep > 0)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> process_mergesmsWarn is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
                sleep(m_pCfg->m_cfgCommon.m_nSleep);
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> process_mergesmsWarn is empty, not need sleep");
            }
            break;
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesmsWarn order : %d orders from table %s.", m_listImsNtfMergeWarn.size(), m_sourceTable.c_str());
        if( !m_listImsNtfMergeWarn.empty() )
        {
            string strExtend9 = m_listImsNtfMergeWarn[0].get_extend9();
            m_listImsNtfMergeWarn.clear();
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms strExtend9 = %s.", strExtend9.c_str());
            query_mergedata( m_listImsNtfMergeWarn, strExtend9 );
            if (m_listImsNtfMergeWarn.empty())
            {
                continue;
            }
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query process_mergesms order : %lld orders from table %s.", m_listImsNtfMergeWarn.size(), m_sourceTable.c_str());
        }

        CMergewarnMap mapCMergeWarn;
        for (MAbmInterfacePromptDb::CImsNtfMergewarnList::const_iterator itrImsNtfMerge = m_listImsNtfMergeWarn.begin(); itrImsNtfMerge != m_listImsNtfMergeWarn.end(); ++itrImsNtfMerge)
        {
            MAbmInterfacePromptDb::CImsNtfMergewarn cimsNtfMergeWarnT = *itrImsNtfMerge;
            LOG_TRACE((cimsNtfMergeWarnT).to_string().c_str());
            int64 llCreateDate = atol64(cimsNtfMergeWarnT.get_createDate().toString("%Y%M%D%H%N%S"));
            int64 llCreateD = llCreateDate/1000000;
            //if(!isToday( llCreateD ) )
            //    continue;
            CMergeCreditKey cMergeWarnKey( cimsNtfMergeWarnT, llCreateD );
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> key acct_id = %lld, cust_id = %lld,create_dt = %lld,phone_id = %s,product_id= %lld", 
                cMergeWarnKey.acct_id, cMergeWarnKey.cust_id,cMergeWarnKey.create_dt,cMergeWarnKey.phone_id.c_str(),cMergeWarnKey.product_id);
        
            CMergewarnMap::iterator iterCMergeStop = mapCMergeWarn.find( cMergeWarnKey );
            if( iterCMergeStop != mapCMergeWarn.end() )
            {
                iterCMergeStop->second.push_back(cimsNtfMergeWarnT);
            }
            else
            {
                MAbmInterfacePromptDb::CImsNtfMergewarnList listCImsNtfMergewarn;
                listCImsNtfMergewarn.push_back(cimsNtfMergeWarnT);
                mapCMergeWarn.insert(AISTD make_pair( cMergeWarnKey, listCImsNtfMergewarn ) );
            }
        }

        for ( CMergewarnMap::iterator iterMgMp = mapCMergeWarn.begin(); iterMgMp != mapCMergeWarn.end(); ++iterMgMp )
        {
            MAbmInterfacePromptDb::CImsNtfMergewarnList &m_listImsNtfMergewarn = iterMgMp->second;
            MAbmInterfacePromptDb::CImsNtfMergewarnHisList MergeDateHisList;
            MAbmInterfacePromptDb::CImsNtfGrpcreditList  GrpDateList;
            mergesms(pSession, m_listImsNtfMergewarn, MergeDateHisList,GrpDateList, cErrorMsg);
            doMergeSmsToDB(m_listImsNtfMergewarn,MergeDateHisList ,GrpDateList,true, pSession, cErrorMsg);
        }
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

//add by xupp3 for yunnanV8 end

int32 ABMPrompt::process_mergecredit(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    MAbmInterfacePromptDb::CImsNtfMergecreditList needSaveData;
    //MAbmInterfacePromptDb::CImsNtfCredit creditSmsList;
    needSaveData.clear();
    m_listImsNtfGrpcredit.clear();
    m_listImsNtfMergeCreditHis.clear();
    int32 iRetValue = ABMPROMPT_OK;
    int32 iCount = 0;

    if ((m_iMergeNum < 10) || (m_iMergeNum > 40))
    {
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> merge_num out of limit,used default!");
        if (m_iMergeNum < 10)
        {
            m_iMergeNum = 10;
        }
        if (m_iMergeNum > 40)
        {
            m_iMergeNum = 40;
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_iMergeNum:%d", m_iMergeNum);

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergecredit->query_data")
    query_data(m_listImsNtfMergeCredit);
    ES_END_RUN_TIME
    if (m_listImsNtfMergeCredit.empty())
    {
        if (m_pCfg->m_cfgCommon.m_nSleep > 0)
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeCredit is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
            sleep(m_pCfg->m_cfgCommon.m_nSleep);
        }
        else
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeCredit is empty, not need sleep");
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_mergecredit order : %d orders from table %s.", m_listImsNtfMergeCredit.size(), m_sourceTable.c_str());
    //MAbmInterfacePromptDb::CImsNtfMergecreditList::iterator itrtemp;
    MAbmInterfacePromptDb::CImsNtfMergecredit cimsNtfMergeCredit;
    for (m_itrImsNtfMergeCredit = m_listImsNtfMergeCredit.begin(); m_itrImsNtfMergeCredit != m_listImsNtfMergeCredit.end(); ++m_itrImsNtfMergeCredit)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergecredit->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        cimsNtfMergeCredit = *m_itrImsNtfMergeCredit;
        if (!needSaveData.empty())
        {
            MAbmInterfacePromptDb::CImsNtfMergecreditList::iterator itrNeed = needSaveData.begin();
            if (cimsNtfMergeCredit.get_seriesId() != itrNeed->get_seriesId())
            { // 不是同一个id，则需要将已保存的数据根据cust_id再次分组，同一个cust_id进行合并
                doGroupCreditData(needSaveData, pSession, cErrorMsg);
                needSaveData.clear();
                needSaveData.push_back(cimsNtfMergeCredit);
            }
            else
            {
                bool isNeed = true;
                for (; itrNeed != needSaveData.end(); ++itrNeed)
                {
                    if (itrNeed->get_resourceId() == cimsNtfMergeCredit.get_resourceId())
                    { // 防止同一个user_id存多次。
                        isNeed = false;
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==>get_resourceId:%lld is same,do not need save!", cimsNtfMergeCredit.get_resourceId());
                        break;
                    }
                }
                if (isNeed)
                {
                    needSaveData.push_back(cimsNtfMergeCredit);
                }
            }
        }
        else
        {
            needSaveData.push_back(cimsNtfMergeCredit);
        }
        // 默认插入历史表都是成功的
        //MAbmInterfacePromptDb::CImsNtfMergecreditHis imsNtfMerageCreditHis;
        //transToHis(cimsNtfMergeCredit, imsNtfMerageCreditHis);
        //imsNtfMerageCreditHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
        //imsNtfMerageCreditHis.set_remark("insert into ims_ntf_credit OK");
        //imsNtfMerageCreditHis.set_soDate(m_dtDateTime);
        //imsNtfMerageCreditHis.set_tfDate(m_dtDateTime);
        //m_listImsNtfMergeCreditHis.push_back(imsNtfMerageCreditHis);

        ES_END_RUN_TIME
    }
    if (!needSaveData.empty())
    { // 认为一次性取出了所有数据，最后剩余的认为就是一组
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> last data size : %d", needSaveData.size());
        m_dtDateTime = CBSDateTime::currentDateTime();
        doGroupCreditData(needSaveData, pSession, cErrorMsg);
        //doMergeCreditDataToDB(needSaveData, pSession, cErrorMsg);
        needSaveData.clear();
    }

    ABM_PROMPT_STAT
    contrl_commit(pSession, cErrorMsg);
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_mergewarn(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    MAbmInterfacePromptDb::CImsNtfMergewarnList needSaveData;
    needSaveData.clear();
    m_listImsNtfGrpcredit.clear();
    m_listImsNtfMergeWarnHis.clear();
    int32 iRetValue = ABMPROMPT_OK;
    int32 iCount = 0;

    if ((m_iMergeNum < 10) || (m_iMergeNum > 40))
    {
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> merge_num out of limit,used default!");
        if (m_iMergeNum < 10)
        {
            m_iMergeNum = 10;
        }
        if (m_iMergeNum > 40)
        {
            m_iMergeNum = 40;
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_iMergeNum:%d", m_iMergeNum);

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergewarn->query_data")
    query_data(m_listImsNtfMergeWarn);
    ES_END_RUN_TIME
    if (m_listImsNtfMergeWarn.empty())
    {
        if (m_pCfg->m_cfgCommon.m_nSleep > 0)
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeWarn is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
            sleep(m_pCfg->m_cfgCommon.m_nSleep);
        }
        else
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeWarn is empty, not need sleep");
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_mergewarn order : %d orders from table %s.", m_listImsNtfMergeWarn.size(), m_sourceTable.c_str());
    MAbmInterfacePromptDb::CImsNtfMergewarn cimsNtfMergeWarn;
    for (m_itrImsNtfMergeWarn = m_listImsNtfMergeWarn.begin(); m_itrImsNtfMergeWarn != m_listImsNtfMergeWarn.end(); ++m_itrImsNtfMergeWarn)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergewarn->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        cimsNtfMergeWarn = *m_itrImsNtfMergeWarn;
        if (!needSaveData.empty())
        {
            MAbmInterfacePromptDb::CImsNtfMergewarnList::iterator itrNeed = needSaveData.begin();
            if (cimsNtfMergeWarn.get_seriesId() != itrNeed->get_seriesId())
            { // 不是同一个id，则需要将已保存的数据进行合并
                doGroupWarnData(needSaveData, pSession, cErrorMsg);
                needSaveData.clear();
                needSaveData.push_back(cimsNtfMergeWarn);
            }
            else
            {
                bool isNeed = true;
                for (; itrNeed != needSaveData.end(); ++itrNeed)
                {
                    if (itrNeed->get_resourceId() == cimsNtfMergeWarn.get_resourceId())
                    { // 防止同一个user_id存多次。
                        isNeed = false;
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==>get_resourceId:%lld is same,do not need save!", cimsNtfMergeWarn.get_resourceId());
                        break;
                    }
                }
                if (isNeed)
                {
                    needSaveData.push_back(cimsNtfMergeWarn);
                }
            }
        }
        else
        {
            needSaveData.push_back(cimsNtfMergeWarn);
        }
        // 默认插入历史表都是成功的
        //MAbmInterfacePromptDb::CImsNtfMergewarnHis imsNtfMerageWarnHis;
        //transToHis(cimsNtfMergeWarn, imsNtfMerageWarnHis);
        //imsNtfMerageWarnHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
        //imsNtfMerageWarnHis.set_remark("insert into ims_ntf_credit OK");
        //imsNtfMerageWarnHis.set_soDate(m_dtDateTime);
        //imsNtfMerageWarnHis.set_tfDate(m_dtDateTime);
        //m_listImsNtfMergeWarnHis.push_back(imsNtfMerageWarnHis);
        ES_END_RUN_TIME
        iCount++;
        if (iCount >= m_pCfg->m_cfgCommon.m_iCommitNum)
        {
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
    }
    if (!needSaveData.empty())
    { // 认为一次性取出了所有数据，最后剩余的认为就是一组
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> last data size : %d", needSaveData.size());
        m_dtDateTime = CBSDateTime::currentDateTime();
        doGroupWarnData(needSaveData, pSession, cErrorMsg);
        needSaveData.clear();
    }

    ABM_PROMPT_STAT
    contrl_commit(pSession, cErrorMsg);

    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::process_mergestop(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    MAbmInterfacePromptDb::CImsNtfMergestopList needSaveData;
    needSaveData.clear();
    m_listImsNtfGrpcredit.clear();
    m_listImsNtfMergeStopHis.clear();
    int32 iRetValue = ABMPROMPT_OK;
    int32 iCount = 0;
    if ((m_iMergeNum < 10) || (m_iMergeNum > 40))
    {
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> merge_num out of limit,used default!");
        if (m_iMergeNum < 10)
        {
            m_iMergeNum = 10;
        }
        if (m_iMergeNum > 40)
        {
            m_iMergeNum = 40;
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_iMergeNum:%d", m_iMergeNum);

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergestop->query_data")
    query_data(m_listImsNtfMergeStop);
    ES_END_RUN_TIME
    if (m_listImsNtfMergeStop.empty())
    {
        if (m_pCfg->m_cfgCommon.m_nSleep > 0)
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeStop is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
            sleep(m_pCfg->m_cfgCommon.m_nSleep);
        }
        else
        {
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfMergeStop is empty, not need sleep");
        }
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_mergestop order : %d orders from table %s.", m_listImsNtfMergeStop.size(), m_sourceTable.c_str());
    MAbmInterfacePromptDb::CImsNtfMergestop cimsNtfMergeStop;
    for (m_itrImsNtfMergeStop = m_listImsNtfMergeStop.begin(); m_itrImsNtfMergeStop != m_listImsNtfMergeStop.end(); ++m_itrImsNtfMergeStop)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_mergestop->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        cimsNtfMergeStop = *m_itrImsNtfMergeStop;
        if (!needSaveData.empty())
        {
            MAbmInterfacePromptDb::CImsNtfMergestopList::iterator itrNeed = needSaveData.begin();
            if (cimsNtfMergeStop.get_seriesId() != itrNeed->get_seriesId())
            { // 不是同一个id，则需要将已保存的数据进行合并
                doGroupStopData(needSaveData, pSession, cErrorMsg);
                needSaveData.clear();
                needSaveData.push_back(cimsNtfMergeStop);
            }
            else
            {
                bool isNeed = true;
                for (; itrNeed != needSaveData.end(); ++itrNeed)
                {
                    if (itrNeed->get_resourceId() == cimsNtfMergeStop.get_resourceId())
                    { // 防止同一个user_id存多次。
                        isNeed = false;
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==>get_resourceId:%lld is same,do not need save!", cimsNtfMergeStop.get_resourceId());
                        break;
                    }
                }
                if (isNeed)
                {
                    needSaveData.push_back(cimsNtfMergeStop);
                }
            }
        }
        else
        {
            needSaveData.push_back(cimsNtfMergeStop);
        }
        ES_END_RUN_TIME
        iCount++;
        if (iCount >= m_pCfg->m_cfgCommon.m_iCommitNum)
        {
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
    }
    if (!needSaveData.empty())
    { // 认为一次性取出了所有数据，最后剩余的认为就是一组
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> last data size : %d", needSaveData.size());
        m_dtDateTime = CBSDateTime::currentDateTime();
        doGroupStopData(needSaveData, pSession, cErrorMsg);
        needSaveData.clear();
    }

    ABM_PROMPT_STAT
    contrl_commit(pSession, cErrorMsg);

    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::doGroupCreditData(MAbmInterfacePromptDb::CImsNtfMergecreditList cinList, SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    aimap<string, MAbmInterfacePromptDb::CImsNtfMergecreditList> mapMergeData;
    MAbmInterfacePromptDb::CImsNtfMergecreditList listMergeData;
    int64 iCustId = 0;
    int16 iMergeCount = 0;
    string strCustGroupId;
    mapMergeData.clear();

    ENTER_FUNC
    if (cinList.empty())
    {
        return ABMPROMPT_OK;
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need to merge data size: %ld", cinList.size());
    MAbmInterfacePromptDb::CImsNtfMergecreditList::iterator itrNeed = cinList.begin();
    for (itrNeed = cinList.begin(); itrNeed != cinList.end(); ++itrNeed)
    {
        if (itrNeed->get_extend1().empty())
        { // extend1字段为空时，默认采用0
            strCustGroupId = "0|0";
        }
        else
        {
            vector<string> vct_baseData;
            vct_baseData.clear();
            cdk::strings::Split(itrNeed->get_extend1(), "|", vct_baseData);
            if (vct_baseData.size() != 2)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> extend1 is error!!!mybe merge error!!!");
                //strCustGroupId = "0|0";
            }
            strCustGroupId = itrNeed->get_extend1();
        }
        aimap<string, MAbmInterfacePromptDb::CImsNtfMergecreditList>::iterator iterMapDataTemp = mapMergeData.find(strCustGroupId);
        if (iterMapDataTemp != mapMergeData.end())
        { // map中存在该cust_id数据，将数据插入list中
            iterMapDataTemp->second.push_back(*itrNeed);
        }
        else
        { // map中没有存放该cust_id数据。新增
            MAbmInterfacePromptDb::CImsNtfMergecreditList listTemp;
            listTemp.clear();
            listTemp.push_back(*itrNeed);
            mapMergeData.insert(AISTD make_pair(strCustGroupId, listTemp));
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> new cust_group_id:%s to map.", strCustGroupId.c_str());
        }
    }

    aimap<string, MAbmInterfacePromptDb::CImsNtfMergecreditList>::iterator iterMapData;
    MAbmInterfacePromptDb::CImsNtfMergecreditList listCreditData;
    for (iterMapData = mapMergeData.begin(); iterMapData != mapMergeData.end(); ++iterMapData)
    { // 取出map中的list数据，进行合并
        iMergeCount = 0;
        listCreditData.clear();
        m_listImsNtfMergeCreditHis.clear();
        m_listImsNtfGroupHis.clear();
		int64 llOldAcctId = 0;
		int64 llOldCustId = 0;
		int32 lOldCountyCode = 0;
        listMergeData = iterMapData->second;
        MAbmInterfacePromptDb::CImsNtfMergecreditList::iterator itrListData = listMergeData.begin();
        for (itrListData = listMergeData.begin(); itrListData != listMergeData.end(); ++itrListData)
        {
            if (iMergeCount == 0)
            {
                listCreditData.clear();
            }
            MAbmInterfacePromptDb::CImsNtfMergecredit imsNtfMergeCredit;
			imsNtfMergeCredit = *itrListData;
			imsNtfMergeCredit.set_remark("ok|ok");// xxx|ok : ok标识该数据已入工单明细表。后面流程不需要再入
            listCreditData.push_back(imsNtfMergeCredit);
            MAbmInterfacePromptDb::CImsNtfMergecreditHis imsNtfMerageCreditHis;
            transToHis(*itrListData, imsNtfMerageCreditHis);
            imsNtfMerageCreditHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
            imsNtfMerageCreditHis.set_remark("insert into ims_ntf_credit OK|ok"); // xxx|ok : ok标识该数据已入工单明细表。后面流程不需要再入
            imsNtfMerageCreditHis.set_soDate(m_dtDateTime);
            imsNtfMerageCreditHis.set_tfDate(m_dtDateTime);
            m_listImsNtfMergeCreditHis.push_back(imsNtfMerageCreditHis);
            if(imsNtfMerageCreditHis.get_productId() > 0)
			{
				int64 llProductId = imsNtfMerageCreditHis.get_productId();
				if(judgeIsShow(llProductId, imsNtfMerageCreditHis.get_regionCode()))
				{// 需要入明细表，则获取相关数据
					MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
					tran2GroupHis(imsNtfMerageCreditHis, imsNtfGroupHis);
					int32 iTradeTypeCode = 0;
					if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
					{
						int64 llCust_id = 0;
						int32 lCountyCode = 0;
						if(llOldAcctId != imsNtfGroupHis.get_acctId())
						{// 减少数据库操作
							getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCust_id, lCountyCode);
							llOldCustId = llCust_id;
							llOldAcctId = imsNtfGroupHis.get_acctId();
							lOldCountyCode = lCountyCode;
						}
						else
						{
							llCust_id = llOldCustId;
							lCountyCode = lOldCountyCode;
						}
						imsNtfGroupHis.set_srcType("MERGE");
						imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
						imsNtfGroupHis.set_custId(llCust_id);
						imsNtfGroupHis.set_assetAmount(0);
						imsNtfGroupHis.set_productId(llProductId);
						imsNtfGroupHis.set_updateTime(m_dtDateTime);
						imsNtfGroupHis.set_countyCode(lCountyCode);
						m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
					}
				}
			}
            iMergeCount++;
            if (iMergeCount >= m_iMergeNum)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need to merge [iMergeCount:%d][m_iMergeNum:%d]", iMergeCount, m_iMergeNum);
                // 进行合并
                if (ABMPROMPT_ERROR == doMergeCreditDataToDB(listCreditData, false, pSession, cErrorMsg))
                { // 合并处理失败，则该cust_id的记录不处理
                    m_listImsNtfGrpcredit.clear();
                    listCreditData.clear();
                    m_listImsNtfMergeCreditHis.clear();
                    m_listImsNtfGroupHis.clear();
                    iMergeCount = 0;
                    break;
                }
                m_listImsNtfGrpcredit.clear();
                //listCreditData.clear();
                iMergeCount = 0;
                contrl_commit(pSession, cErrorMsg); // 合并一次，可能存在多条，提交一次，
            }
        }
        if (!listCreditData.empty())
        {
            if (iMergeCount == 0) // 这种情况，表示合并的数据量正好是 m_iMergeNum 的整数倍。此时需要删除数据就行
            {
                m_imsNtfMergeCredit.set_seriesId(listCreditData[0].get_seriesId());
                m_imsNtfMergeCredit.set_extend1(listCreditData[0].get_extend1());
                if (delete_dataMergeCredit(pSession, cErrorMsg) != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    m_imsNtfMergeCredit.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfMergeCredit.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfMergeCredit.set_soDate(m_dtDateTime);
                    update_dataMergeCredit(pSession, cErrorMsg);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id=%lld extend1=%s come from MergeCredit sub_table ,%s : %s .",
                              m_imsNtfMergeCredit.get_seriesId(), m_imsNtfMergeCredit.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                    return ABMPROMPT_ERROR;
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> series_id=%lld extend1=%s come from MergeCredit sub_table is done successed",
                              m_imsNtfMergeCredit.get_seriesId(), m_imsNtfMergeCredit.get_extend1().c_str());
                }
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> last data size : %d ...", listCreditData.size());
                doMergeCreditDataToDB(listCreditData, true, pSession, cErrorMsg);
                m_listImsNtfGrpcredit.clear();
                listCreditData.clear();
                m_listImsNtfMergeCreditHis.clear();
                m_listImsNtfGroupHis.clear();
                iMergeCount = 0;
            }
            contrl_commit(pSession, cErrorMsg);
        }
    }

    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::insertIntoSmsWechat(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    if (m_listTiOSmsWechat.empty() == true)
    {
        return ABMPROMPT_OK;
    }
    //int32 iret = ABMPROMPT_OK;
    try
    {
        ob_kernel<MAbmInterfacePromptDb::CTiOSmsWechatList> ins("ZG");
        ins.insert(m_listTiOSmsWechat, false);
    }
    catch (err_info_service::CAIException &e)
    {
        LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , catch exception: Code: %lld, message: %s, logrecord: %s",
                  __func__, e.get_code(), e.get_message().c_str(), e.get_logrecord().c_str());
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(int64(e.get_code())));
        cErrorMsg.set_hint(e.get_message().substr(0, 2048));
        return ABMPROMPT_DB_OP_ERR;
        //throw;
    }
    catch (otl_exception &e)
    {
        AISTD string strOtl = AISTD string("error_msg=\notl_exception:") + AISTD string((char *)e.msg) + AISTD string(e.stm_text) + AISTD string(e.var_info);
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==>func:%s , Exception: %s", __func__, strOtl.c_str());
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
        cErrorMsg.set_hint(strOtl.substr(0, 2048));
        return ABMPROMPT_DB_OP_ERR;
    }
    catch (...)
    {
        LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , catch unknow exception!!!========", __func__);
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
        cErrorMsg.set_hint("insert into ti_o_sms_wechat catch unknown exception ERROR!");
        return ABMPROMPT_DB_OP_ERR;
    }

    return ABMPROMPT_OK;
}

int32 ABMPrompt::insert_dataToHisWechat(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    if (m_listImsNtfWechatHis.empty())
    {
        return ABMPROMPT_OK;
    }
    TRY_BEGIN
    ob_kernel<MAbmInterfacePromptDb::CImsNtfWechatHisList> ins("JD");
    ins.insert(m_listImsNtfWechatHis, false, (m_hisTable + m_listImsNtfWechatHis[0].get_soDate().toString("%M")).c_str());
    CATCH_END_NOTHROW

    return ABMPROMPT_OK;
}

int32 ABMPrompt::doGroupWarnData(MAbmInterfacePromptDb::CImsNtfMergewarnList cinList, SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    aimap<string, MAbmInterfacePromptDb::CImsNtfMergewarnList> mapMergeData;
    MAbmInterfacePromptDb::CImsNtfMergewarnList listMergeData;
    int64 iCustId = 0;
    int16 iMergeCount = 0;
    string strCustGroupId;
    mapMergeData.clear();

    ENTER_FUNC
    if (cinList.empty())
    {
        return ABMPROMPT_OK;
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need to merge data size: %ld", cinList.size());
    MAbmInterfacePromptDb::CImsNtfMergewarnList::iterator itrNeed = cinList.begin();
    for (itrNeed = cinList.begin(); itrNeed != cinList.end(); ++itrNeed)
    {
        if (itrNeed->get_extend1().empty())
        { // extend1字段为空时，默认采用0
            strCustGroupId = "0|0";
        }
        else
        {
            vector<string> vct_baseData;
            vct_baseData.clear();
            cdk::strings::Split(itrNeed->get_extend1(), "|", vct_baseData);
            if (vct_baseData.size() != 2)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> extend1 is error!!!mybe merge error!!!");
                //strCustGroupId = "0|0";
            }
            strCustGroupId = itrNeed->get_extend1();
        }
        aimap<string, MAbmInterfacePromptDb::CImsNtfMergewarnList>::iterator iterMapDataTemp = mapMergeData.find(strCustGroupId);
        if (iterMapDataTemp != mapMergeData.end())
        { // map中存在该cust_id数据，将数据插入list中
            iterMapDataTemp->second.push_back(*itrNeed);
        }
        else
        { // map中没有存放该cust_id数据。新增
            MAbmInterfacePromptDb::CImsNtfMergewarnList listTemp;
            listTemp.clear();
            listTemp.push_back(*itrNeed);
            mapMergeData.insert(AISTD make_pair(strCustGroupId, listTemp));
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> new cust_group_id:%s to map.", strCustGroupId.c_str());
        }
    }

    aimap<string, MAbmInterfacePromptDb::CImsNtfMergewarnList>::iterator iterMapData;
    MAbmInterfacePromptDb::CImsNtfMergewarnList listCreditData;
    for (iterMapData = mapMergeData.begin(); iterMapData != mapMergeData.end(); ++iterMapData)
    { // 取出map中的list数据，进行合并
        iMergeCount = 0;
        listCreditData.clear();
        m_listImsNtfMergeWarnHis.clear();
        m_listImsNtfGroupHis.clear();
		int64 llOldAcctId = 0;
		int64 llOldCustId = 0;
		int32 lOldCountyCode = 0;
        listMergeData = iterMapData->second;
        MAbmInterfacePromptDb::CImsNtfMergewarnList::iterator itrListData = listMergeData.begin();
        for (itrListData = listMergeData.begin(); itrListData != listMergeData.end(); ++itrListData)
        {
            if (iMergeCount == 0)
            {
                listCreditData.clear();
            }
            MAbmInterfacePromptDb::CImsNtfMergewarn imsNtfMergeWarn;
			imsNtfMergeWarn = *itrListData;
			imsNtfMergeWarn.set_remark("ok|ok");// xxx|ok : ok标识该数据已入工单明细表。后面流程不需要再入
			listCreditData.push_back(imsNtfMergeWarn);
            MAbmInterfacePromptDb::CImsNtfMergewarnHis imsNtfMerageCreditHis;
            transToHis(*itrListData, imsNtfMerageCreditHis);
            imsNtfMerageCreditHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
            imsNtfMerageCreditHis.set_remark("insert into ims_ntf_credit OK|ok");// xxx|ok : ok标识该数据已入工单明细表。后面流程不需要再入
            imsNtfMerageCreditHis.set_soDate(m_dtDateTime);
            imsNtfMerageCreditHis.set_tfDate(m_dtDateTime);
            m_listImsNtfMergeWarnHis.push_back(imsNtfMerageCreditHis);
            if(imsNtfMerageCreditHis.get_productId() > 0)
			{
				int64 llProductId = imsNtfMerageCreditHis.get_productId();
				if(judgeIsShow(llProductId, imsNtfMerageCreditHis.get_regionCode()))
				{// 需要入明细表，则获取相关数据
					MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
					tran2GroupHis(imsNtfMerageCreditHis, imsNtfGroupHis);
					int32 iTradeTypeCode = 0;
					if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
					{
						int64 llCust_id = 0;
						int32 lCountyCode = 0;
						if(llOldAcctId != imsNtfGroupHis.get_acctId())
						{// 减少数据库操作
							getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCust_id, lCountyCode);
							llOldCustId = llCust_id;
							llOldAcctId = imsNtfGroupHis.get_acctId();
							lOldCountyCode = lCountyCode;
						}
						else
						{
							llCust_id = llOldCustId;
							lCountyCode = lOldCountyCode;
						}
						imsNtfGroupHis.set_srcType("MERGE");
						imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
						imsNtfGroupHis.set_custId(llCust_id);
						imsNtfGroupHis.set_assetAmount(0);
						imsNtfGroupHis.set_productId(llProductId);
						imsNtfGroupHis.set_updateTime(m_dtDateTime);
						imsNtfGroupHis.set_countyCode(lCountyCode);
						m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
					}
				}
			}
            iMergeCount++;
            if (iMergeCount >= m_iMergeNum)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need to merge [iMergeCount:%d][m_iMergeNum:%d]", iMergeCount, m_iMergeNum);
                // 进行合并
                if (ABMPROMPT_ERROR == doMergeWarnDataToDB(listCreditData, false, pSession, cErrorMsg))
                { // 合并处理失败，则该cust_id的记录不处理
                    m_listImsNtfGrpcredit.clear();
                    listCreditData.clear();
                    m_listImsNtfMergeWarnHis.clear();
                    m_listImsNtfGroupHis.clear();
                    iMergeCount = 0;
                    break;
                }
                m_listImsNtfGrpcredit.clear();
                //listCreditData.clear();
                iMergeCount = 0;
                contrl_commit(pSession, cErrorMsg); // 合并一次，可能存在多条，提交一次，
            }
        }
        if (!listCreditData.empty())
        {
            if (iMergeCount == 0) // 这种情况，表示合并的数据量正好是 m_iMergeNum 的整数倍。此时需要删除数据就行
            {
                m_imsNtfMergeWarn.set_seriesId(listCreditData[0].get_seriesId());
                m_imsNtfMergeWarn.set_extend1(listCreditData[0].get_extend1());
                if (delete_dataMergeWarn(pSession, cErrorMsg) != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    m_imsNtfMergeWarn.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfMergeWarn.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfMergeWarn.set_soDate(m_dtDateTime);
                    update_dataMergeWarn(pSession, cErrorMsg);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table ,%s : %s .",
                              m_imsNtfMergeWarn.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table is done successed",
                              m_imsNtfMergeWarn.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str());
                }
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> last data size : %d ...", listCreditData.size());
                doMergeWarnDataToDB(listCreditData, true, pSession, cErrorMsg);
                m_listImsNtfGrpcredit.clear();
                listCreditData.clear();
                m_listImsNtfMergeWarnHis.clear();
                m_listImsNtfGroupHis.clear();
                iMergeCount = 0;
            }
            contrl_commit(pSession, cErrorMsg);
        }
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::doGroupStopData(MAbmInterfacePromptDb::CImsNtfMergestopList cinList, SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    aimap<string, MAbmInterfacePromptDb::CImsNtfMergestopList> mapMergeData;
    MAbmInterfacePromptDb::CImsNtfMergestopList listMergeData;
    int64 iCustId = 0;
    int16 iMergeCount = 0;
    string strCustGroupId;
    mapMergeData.clear();

    ENTER_FUNC
    if (cinList.empty())
    {
        return ABMPROMPT_OK;
    }
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need to merge data size: %ld", cinList.size());
    MAbmInterfacePromptDb::CImsNtfMergestopList::iterator itrNeed = cinList.begin();
    for (itrNeed = cinList.begin(); itrNeed != cinList.end(); ++itrNeed)
    {
        if (itrNeed->get_extend1().empty())
        { // extend1字段为空时，默认采用0
            strCustGroupId = "0|0";
        }
        else
        {
            vector<string> vct_baseData;
            vct_baseData.clear();
            cdk::strings::Split(itrNeed->get_extend1(), "|", vct_baseData);
            if (vct_baseData.size() != 2)
            {
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> extend1 is error!!!mybe merge error!!!");
                //strCustGroupId = "0|0";
            }
            strCustGroupId = itrNeed->get_extend1();
        }
        aimap<string, MAbmInterfacePromptDb::CImsNtfMergestopList>::iterator iterMapDataTemp = mapMergeData.find(strCustGroupId);
        if (iterMapDataTemp != mapMergeData.end())
        { // map中存在该cust_id数据，将数据插入list中
            iterMapDataTemp->second.push_back(*itrNeed);
        }
        else
        { // map中没有存放该cust_id数据。新增
            MAbmInterfacePromptDb::CImsNtfMergestopList listTemp;
            listTemp.clear();
            listTemp.push_back(*itrNeed);
            mapMergeData.insert(AISTD make_pair(strCustGroupId, listTemp));
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> new cust_group_id:%s to map.", strCustGroupId.c_str());
        }
    }

    aimap<string, MAbmInterfacePromptDb::CImsNtfMergestopList>::iterator iterMapData;
    MAbmInterfacePromptDb::CImsNtfMergestopList listCreditData;
    for (iterMapData = mapMergeData.begin(); iterMapData != mapMergeData.end(); ++iterMapData)
    { // 取出map中的list数据，进行合并
        iMergeCount = 0;
        listCreditData.clear();
        m_listImsNtfMergeStopHis.clear();
        m_listImsNtfGroupHis.clear();
		int64 llOldAcctId = 0;
		int64 llOldCustId = 0;
		int32 lOldCountyCode = 0;
        listMergeData = iterMapData->second;
        MAbmInterfacePromptDb::CImsNtfMergestopList::iterator itrListData = listMergeData.begin();
        for (itrListData = listMergeData.begin(); itrListData != listMergeData.end(); ++itrListData)
        {
            if (iMergeCount == 0)
            {
                listCreditData.clear();
            }
            MAbmInterfacePromptDb::CImsNtfMergestop imsNtfMergeStop;
			imsNtfMergeStop = *itrListData;
			imsNtfMergeStop.set_remark("ok|ok");// xxx|ok : ok标识该数据已入工单明细表。后面流程不需要再入
			listCreditData.push_back(imsNtfMergeStop);
            MAbmInterfacePromptDb::CImsNtfMergestopHis imsNtfMerageCreditHis;
            transToHis(*itrListData, imsNtfMerageCreditHis);
            imsNtfMerageCreditHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
            imsNtfMerageCreditHis.set_remark("insert into ims_ntf_credit OK|ok");// xxx|ok : ok标识该数据已入工单明细表。后面流程不需要再入
            imsNtfMerageCreditHis.set_soDate(m_dtDateTime);
            imsNtfMerageCreditHis.set_tfDate(m_dtDateTime);
            m_listImsNtfMergeStopHis.push_back(imsNtfMerageCreditHis);
            if(imsNtfMerageCreditHis.get_productId() > 0)
			{
				int64 llProductId = imsNtfMerageCreditHis.get_productId();
				if(judgeIsShow(llProductId, imsNtfMerageCreditHis.get_regionCode()))
				{// 需要入明细表，则获取相关数据
					MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
					tran2GroupHis(imsNtfMerageCreditHis, imsNtfGroupHis);
					int32 iTradeTypeCode = 0;
					if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
					{
						int64 llCust_id = 0;
						int32 lCountyCode = 0;
						if(llOldAcctId != imsNtfGroupHis.get_acctId())
						{// 减少数据库操作
							getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCust_id, lCountyCode);
							llOldCustId = llCust_id;
							llOldAcctId = imsNtfGroupHis.get_acctId();
							lOldCountyCode = lCountyCode;
						}
						else
						{
							llCust_id = llOldCustId;
							lCountyCode = lOldCountyCode;
						}
						imsNtfGroupHis.set_srcType("MERGE");
						imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
						imsNtfGroupHis.set_custId(llCust_id);
						imsNtfGroupHis.set_assetAmount(0);
						imsNtfGroupHis.set_productId(llProductId);
						imsNtfGroupHis.set_updateTime(m_dtDateTime);
						imsNtfGroupHis.set_countyCode(lCountyCode);
						m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
					}
				}
			}
            iMergeCount++;
            if (iMergeCount >= m_iMergeNum)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> need to merge [iMergeCount:%d][m_iMergeNum:%d]", iMergeCount, m_iMergeNum);
                // 进行合并
                if (ABMPROMPT_ERROR == doMergeStopDataToDB(listCreditData, false, pSession, cErrorMsg))
                { // 合并处理失败，则该cust_id的记录不处理
                    m_listImsNtfGrpcredit.clear();
                    listCreditData.clear();
                    m_listImsNtfMergeStopHis.clear();
                    m_listImsNtfGroupHis.clear();
                    iMergeCount = 0;
                    break;
                }
                m_listImsNtfGrpcredit.clear();
                //listCreditData.clear();
                iMergeCount = 0;
                contrl_commit(pSession, cErrorMsg); // 合并一次，可能存在多条，提交一次，
            }
        }
        if (!listCreditData.empty())
        {
            if (iMergeCount == 0) // 这种情况，表示合并的数据量正好是 m_iMergeNum 的整数倍。此时需要删除数据就行
            {
                m_imsNtfMergeStop.set_seriesId(listCreditData[0].get_seriesId());
                m_imsNtfMergeStop.set_extend1(listCreditData[0].get_extend1());
                if (delete_dataMergeStop(pSession, cErrorMsg) != ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    m_imsNtfMergeStop.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfMergeStop.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfMergeStop.set_soDate(m_dtDateTime);
                    update_dataMergeStop(pSession, cErrorMsg);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table ,%s : %s .",
                              m_imsNtfMergeStop.get_seriesId(), m_imsNtfMergeStop.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table is done successed",
                              m_imsNtfMergeStop.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str());
                }
            }
            else
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> last data size : %d ...", listCreditData.size());
                doMergeStopDataToDB(listCreditData, true, pSession, cErrorMsg);
                m_listImsNtfGrpcredit.clear();
                listCreditData.clear();
                m_listImsNtfMergeStopHis.clear();
                m_listImsNtfGroupHis.clear();
                iMergeCount = 0;
            }
            contrl_commit(pSession, cErrorMsg);
        }
    }
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::doMergeCreditDataToDB(MAbmInterfacePromptDb::CImsNtfMergecreditList cinList, bool isDelete, SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC
    int32 iRetValue = ABMPROMPT_OK;

    if (cinList.empty())
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> cinList is empty,do not to db!");
        return ABMPROMPT_OK;
    }
    iRetValue = mergeGrpcreditSms(cinList, m_listImsNtfGrpcredit, pSession, cErrorMsg);
    MAbmInterfacePromptDb::CImsNtfMergecreditList::iterator itrNeed = cinList.begin();
    if (ABMPROMPT_OK != iRetValue)
    { // 合并失败，需要更新状态。
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfMergeCredit.set_status(ABM_PROMPT_PROCESS_FAILED);
        m_imsNtfMergeCredit.set_seriesId(itrNeed->get_seriesId());
        AISTD string strRemark(cErrorMsg.get_errorMsg());
        strRemark.append(":");
        strRemark.append(cErrorMsg.get_hint());
        m_imsNtfMergeCredit.set_remark(strRemark.substr(0, 1024));
        m_imsNtfMergeCredit.set_soDate(m_dtDateTime);
        m_imsNtfMergeCredit.set_extend1(itrNeed->get_extend1());
        update_dataMergeCredit(pSession, cErrorMsg);
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeCredit sub_table ,%s : %s .",
                  m_imsNtfMergeCredit.get_seriesId(), m_imsNtfMergeCredit.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
        return ABMPROMPT_ERROR;
    }
    else
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->doMergeCreditDataToDB->insertIntoCredit")
        iRetValue = insertIntoGrpcredit(pSession, cErrorMsg);
        ES_END_RUN_TIME
        if (ABMPROMPT_OK == iRetValue)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->doMergeCreditDataToDB->delete_inserthis")
            if (ABMPROMPT_OK == insert_dataToHisMergeCredit(pSession, cErrorMsg))
            {
            	if(ABMPROMPT_OK == insert_dataToHisGroup(pSession, cErrorMsg)) // 增加工单入明细表
            	{
	                m_imsNtfMergeCredit.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
	                m_imsNtfMergeCredit.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
	                if (isDelete)
	                {
	                    if (delete_dataMergeCredit(pSession, cErrorMsg) != ABMPROMPT_OK)
	                    {
	                        contrl_rollback(pSession, cErrorMsg);
	                        m_imsNtfMergeCredit.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
	                        AISTD string strRemark(cErrorMsg.get_errorMsg());
	                        strRemark.append(":");
	                        strRemark.append(cErrorMsg.get_hint());
	                        m_imsNtfMergeCredit.set_remark(strRemark.substr(0, 1024));
	                        m_imsNtfMergeCredit.set_soDate(m_dtDateTime);
	                        update_dataMergeCredit(pSession, cErrorMsg);
	                        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id=%lld extend1=%s come from MergeCredit sub_table ,%s : %s .",
	                                  m_imsNtfMergeCredit.get_seriesId(), m_imsNtfMergeCredit.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
	                        contrl_commit(pSession, cErrorMsg);
	                        return ABMPROMPT_ERROR;
	                    }
	                    else
	                    {
	                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> series_id=%lld extend1=%s come from MergeCredit sub_table is done successed",
	                                  m_imsNtfMergeCredit.get_seriesId(), m_imsNtfMergeCredit.get_extend1().c_str());
	                    }
	                }
                }
                else
                {
                	contrl_rollback(pSession, cErrorMsg);
					m_imsNtfMergeCredit.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
					m_imsNtfMergeCredit.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
					AISTD string strRemark(cErrorMsg.get_errorMsg());
					strRemark.append(":");
					strRemark.append(cErrorMsg.get_hint());
					m_imsNtfMergeCredit.set_remark(strRemark.substr(0, 1024));
					m_imsNtfMergeCredit.set_soDate(m_dtDateTime);
					m_imsNtfMergeCredit.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
					update_dataMergeCredit(pSession, cErrorMsg);
					contrl_commit(pSession, cErrorMsg);
					LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeCredit sub table, %s : %s return to ims_ntf_group_his",
						m_imsNtfMergeCredit.get_seriesId(),
						m_imsNtfMergeCredit.get_extend1().c_str(),
						cErrorMsg.get_errorMsg().c_str(),
						cErrorMsg.get_hint().c_str());
					return ABMPROMPT_ERROR;
                }
            }
            else
            {
                contrl_rollback(pSession, cErrorMsg);
                m_imsNtfMergeCredit.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
                m_imsNtfMergeCredit.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfMergeCredit.set_remark(strRemark.substr(0, 1024));
                m_imsNtfMergeCredit.set_soDate(m_dtDateTime);
                m_imsNtfMergeCredit.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
                update_dataMergeCredit(pSession, cErrorMsg);
                contrl_commit(pSession, cErrorMsg);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeCredit sub table, %s : %s return to ims_ntf_credit",
                          m_imsNtfMergeCredit.get_seriesId(),
                          m_imsNtfMergeCredit.get_extend1().c_str(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
                return ABMPROMPT_ERROR;
            }
            ES_END_RUN_TIME
        }
        else
        {
            m_imsNtfMergeCredit.set_status(ABM_PROMPT_PROCESS_FAILED);
            m_imsNtfMergeCredit.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfMergeCredit.set_remark(strRemark.substr(0, 1024));
            m_imsNtfMergeCredit.set_soDate(m_dtDateTime);
            m_imsNtfMergeCredit.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
            update_dataMergeCredit(pSession, cErrorMsg);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeCredit sub_table ,%s : %s .",
                      m_imsNtfMergeCredit.get_seriesId(), m_imsNtfMergeCredit.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
            return ABMPROMPT_ERROR;
        }
    }
    m_listImsNtfGrpcredit.clear();
    m_listImsNtfMergeCreditHis.clear();
    m_listImsNtfGroupHis.clear();
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::doMergeWarnDataToDB(MAbmInterfacePromptDb::CImsNtfMergewarnList cinList, bool isDelete, SOBSession* pSession, CBSErrorMsg& cErrorMsg)
{
	ENTER_FUNC
	int32 iRetValue = ABMPROMPT_OK;

	if(cinList.empty())
	{
		LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> cinList is empty,do not to db!");
		return ABMPROMPT_OK;
	}
	iRetValue = mergeGrpcreditSms(cinList,m_listImsNtfGrpcredit,pSession,cErrorMsg);
	MAbmInterfacePromptDb::CImsNtfMergewarnList::iterator itrNeed = cinList.begin();
	if(ABMPROMPT_OK != iRetValue)
	{// 合并失败，需要更新状态。
		m_dtDateTime = CBSDateTime::currentDateTime();
		m_imsNtfMergeWarn.set_status(ABM_PROMPT_PROCESS_FAILED);
		m_imsNtfMergeWarn.set_seriesId(itrNeed->get_seriesId());
		AISTD string strRemark(cErrorMsg.get_errorMsg());
		strRemark.append(":");
		strRemark.append(cErrorMsg.get_hint());
		m_imsNtfMergeWarn.set_remark(strRemark.substr(0,2048));
		m_imsNtfMergeWarn.set_soDate(m_dtDateTime);
		m_imsNtfMergeWarn.set_extend1(itrNeed->get_extend1());
		update_dataMergeWarn(pSession, cErrorMsg);
		LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table ,%s : %s .",
			m_imsNtfMergeWarn.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
		return ABMPROMPT_ERROR;
	}
	else
	{
		ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->doMergeWarnDataToDB->insertIntoCredit")
		iRetValue = insertIntoGrpcredit(pSession, cErrorMsg);
		ES_END_RUN_TIME
		if(ABMPROMPT_OK == iRetValue)
		{
			ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->doMergeWarnDataToDB->delete_inserthis")
			if(ABMPROMPT_OK == insert_dataToHisMergeWarn(pSession, cErrorMsg))
			{
				if(ABMPROMPT_OK == insert_dataToHisGroup(pSession, cErrorMsg)) // 增加工单入明细表
				{
					m_imsNtfMergeWarn.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
					m_imsNtfMergeWarn.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
					if(isDelete)
					{
						if (delete_dataMergeWarn(pSession, cErrorMsg) != ABMPROMPT_OK)
						{
							contrl_rollback(pSession, cErrorMsg);
							m_imsNtfMergeWarn.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
							AISTD string strRemark(cErrorMsg.get_errorMsg());
							strRemark.append(":");
							strRemark.append(cErrorMsg.get_hint());
							m_imsNtfMergeWarn.set_remark(strRemark.substr(0, 1024));
							m_imsNtfMergeWarn.set_soDate(m_dtDateTime);

							update_dataMergeWarn(pSession, cErrorMsg);
							LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table ,%s : %s .",
								m_imsNtfMergeWarn.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
							contrl_commit(pSession, cErrorMsg);
							return ABMPROMPT_ERROR;
						}
						else
						{
							LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub_table is done successed",
								m_imsNtfMergeWarn.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str());
						}
					}
				}
				else
				{
					contrl_rollback(pSession, cErrorMsg);
					m_imsNtfMergeWarn.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
					m_imsNtfMergeWarn.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
					AISTD string strRemark(cErrorMsg.get_errorMsg());
					strRemark.append(":");
					strRemark.append(cErrorMsg.get_hint());
					m_imsNtfMergeWarn.set_remark(strRemark.substr(0, 1024));
					m_imsNtfMergeWarn.set_soDate(m_dtDateTime);
					m_imsNtfMergeWarn.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
					update_dataMergeWarn(pSession, cErrorMsg);
					contrl_commit(pSession, cErrorMsg);
					LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub table, %s : %s return to ims_ntf_group_his",
						m_imsNtfMergeWarn.get_seriesId(),
						m_imsNtfMergeWarn.get_extend1().c_str(),
						cErrorMsg.get_errorMsg().c_str(),
						cErrorMsg.get_hint().c_str());
					return ABMPROMPT_ERROR;
				}
			}
			else
			{
				contrl_rollback(pSession, cErrorMsg);
				m_imsNtfMergeWarn.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
				m_imsNtfMergeWarn.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
				AISTD string strRemark(cErrorMsg.get_errorMsg());
				strRemark.append(":");
				strRemark.append(cErrorMsg.get_hint());
				m_imsNtfMergeWarn.set_remark(strRemark.substr(0, 1024));
				m_imsNtfMergeWarn.set_soDate(m_dtDateTime);
				m_imsNtfMergeWarn.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
				update_dataMergeWarn(pSession, cErrorMsg);
				contrl_commit(pSession, cErrorMsg);
				LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeWarn sub table, %s : %s return to ims_ntf_credit",
					m_imsNtfMergeWarn.get_seriesId(),
					m_imsNtfMergeWarn.get_extend1().c_str(),
					cErrorMsg.get_errorMsg().c_str(),
					cErrorMsg.get_hint().c_str());
				return ABMPROMPT_ERROR;
			}
			ES_END_RUN_TIME
		}
		else
		{
			m_imsNtfMergeWarn.set_status(ABM_PROMPT_PROCESS_FAILED);
			m_imsNtfMergeWarn.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
			AISTD string strRemark(cErrorMsg.get_errorMsg());
			strRemark.append(":");
			strRemark.append(cErrorMsg.get_hint());
			m_imsNtfMergeWarn.set_remark(strRemark.substr(0,2048));
			m_imsNtfMergeWarn.set_soDate(m_dtDateTime);
			m_imsNtfMergeWarn.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
			update_dataMergeWarn(pSession, cErrorMsg);
			LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s  come from MergeWarn sub_table ,%s : %s .",
				m_imsNtfMergeWarn.get_seriesId(), m_imsNtfMergeWarn.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
			return ABMPROMPT_ERROR;
		}
	}
	m_listImsNtfGrpcredit.clear();
	m_listImsNtfMergeWarnHis.clear();
	m_listImsNtfGroupHis.clear();
	LEAVE_FUNC
	return ABMPROMPT_OK;
}


int32 ABMPrompt::doMergeStopDataToDB(MAbmInterfacePromptDb::CImsNtfMergestopList cinList, bool isDelete, SOBSession* pSession, CBSErrorMsg& cErrorMsg)
{
	ENTER_FUNC
	int32 iRetValue = ABMPROMPT_OK;

	if(cinList.empty())
	{
		LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> cinList is empty,do not to db!");
		return ABMPROMPT_OK;
	}
	iRetValue = mergeGrpcreditSms(cinList,m_listImsNtfGrpcredit,pSession,cErrorMsg);
	MAbmInterfacePromptDb::CImsNtfMergestopList::iterator itrNeed = cinList.begin();
	if(ABMPROMPT_OK != iRetValue)
	{// 合并失败，需要更新状态。
		m_dtDateTime = CBSDateTime::currentDateTime();
		m_imsNtfMergeStop.set_status(ABM_PROMPT_PROCESS_FAILED);
		m_imsNtfMergeStop.set_seriesId(itrNeed->get_seriesId());
		AISTD string strRemark(cErrorMsg.get_errorMsg());
		strRemark.append(":");
		strRemark.append(cErrorMsg.get_hint());
		m_imsNtfMergeStop.set_remark(strRemark.substr(0,2048));
		m_imsNtfMergeStop.set_soDate(m_dtDateTime);
		m_imsNtfMergeStop.set_extend1(itrNeed->get_extend1());
		update_dataMergeStop(pSession, cErrorMsg);
		LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeStop sub_table ,%s : %s .",
			m_imsNtfMergeStop.get_seriesId(), m_imsNtfMergeStop.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
		return ABMPROMPT_ERROR;
	}
	else
	{
		ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->doMergeStopDataToDB->insertIntoCredit")
		iRetValue = insertIntoGrpcredit(pSession, cErrorMsg);
		ES_END_RUN_TIME
		if(ABMPROMPT_OK == iRetValue)
		{
			ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->doMergeStopDataToDB->delete_inserthis")
			if(ABMPROMPT_OK == insert_dataToHisMergeStop(pSession, cErrorMsg))
			{
				if(ABMPROMPT_OK == insert_dataToHisGroup(pSession, cErrorMsg)) // 增加工单入明细表
				{
					m_imsNtfMergeStop.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
					m_imsNtfMergeStop.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
					if(isDelete)
					{
						if (delete_dataMergeStop(pSession, cErrorMsg) != ABMPROMPT_OK)
						{
							contrl_rollback(pSession, cErrorMsg);
							m_imsNtfMergeStop.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
							AISTD string strRemark(cErrorMsg.get_errorMsg());
							strRemark.append(":");
							strRemark.append(cErrorMsg.get_hint());
							m_imsNtfMergeStop.set_remark(strRemark.substr(0, 1024));
							m_imsNtfMergeStop.set_soDate(m_dtDateTime);

							update_dataMergeStop(pSession, cErrorMsg);
							LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeStop sub_table ,%s : %s .",
								m_imsNtfMergeStop.get_seriesId(), m_imsNtfMergeStop.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
							contrl_commit(pSession, cErrorMsg);
							return ABMPROMPT_ERROR;
						}
						else
						{
							LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeStop sub_table is done successed",
								m_imsNtfMergeStop.get_seriesId(), m_imsNtfMergeStop.get_extend1().c_str());
						}
					}
				}
				else
				{
					contrl_rollback(pSession, cErrorMsg);
					m_imsNtfMergeStop.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
					m_imsNtfMergeStop.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
					AISTD string strRemark(cErrorMsg.get_errorMsg());
					strRemark.append(":");
					strRemark.append(cErrorMsg.get_hint());
					m_imsNtfMergeStop.set_remark(strRemark.substr(0, 1024));
					m_imsNtfMergeStop.set_soDate(m_dtDateTime);
					m_imsNtfMergeStop.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
					update_dataMergeStop(pSession, cErrorMsg);
					contrl_commit(pSession, cErrorMsg);
					LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeStop sub table, %s : %s return to ims_ntf_group_his",
						m_imsNtfMergeStop.get_seriesId(),
						m_imsNtfMergeStop.get_extend1().c_str(),
						cErrorMsg.get_errorMsg().c_str(),
						cErrorMsg.get_hint().c_str());
					return ABMPROMPT_ERROR;
				}
			}
			else
			{
				contrl_rollback(pSession, cErrorMsg);
				m_imsNtfMergeStop.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
				m_imsNtfMergeStop.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
				AISTD string strRemark(cErrorMsg.get_errorMsg());
				strRemark.append(":");
				strRemark.append(cErrorMsg.get_hint());
				m_imsNtfMergeStop.set_remark(strRemark.substr(0, 1024));
				m_imsNtfMergeStop.set_soDate(m_dtDateTime);
				m_imsNtfMergeStop.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
				update_dataMergeStop(pSession, cErrorMsg);
				contrl_commit(pSession, cErrorMsg);
				LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s come from MergeStop sub table, %s : %s return to ims_ntf_credit",
					m_imsNtfMergeStop.get_seriesId(),
					m_imsNtfMergeStop.get_extend1().c_str(),
					cErrorMsg.get_errorMsg().c_str(),
					cErrorMsg.get_hint().c_str());
				return ABMPROMPT_ERROR;
			}
			ES_END_RUN_TIME
		}
		else
		{
			m_imsNtfMergeStop.set_status(ABM_PROMPT_PROCESS_FAILED);
			m_imsNtfMergeStop.set_seriesId(m_listImsNtfGrpcredit[0].get_seriesId());
			AISTD string strRemark(cErrorMsg.get_errorMsg());
			strRemark.append(":");
			strRemark.append(cErrorMsg.get_hint());
			m_imsNtfMergeStop.set_remark(strRemark.substr(0,2048));
			m_imsNtfMergeStop.set_soDate(m_dtDateTime);
			m_imsNtfMergeStop.set_extend1(m_listImsNtfGrpcredit[0].get_extend1());
			update_dataMergeStop(pSession, cErrorMsg);
			LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> series_id =%lld extend1=%s  come from MergeStop sub_table ,%s : %s .",
				m_imsNtfMergeStop.get_seriesId(), m_imsNtfMergeStop.get_extend1().c_str(), cErrorMsg.get_errorMsg().c_str(), cErrorMsg.get_hint().c_str());
			return ABMPROMPT_ERROR;
		}
	}
	m_listImsNtfGrpcredit.clear();
	m_listImsNtfMergeStopHis.clear();
	m_listImsNtfGroupHis.clear();
	LEAVE_FUNC
	return ABMPROMPT_OK;
}


int32 ABMPrompt::process_toread(SOBSession *pSession, CBSErrorMsg &cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_toread->query_data")
    query_data(m_listImsNtfToread);
    ES_END_RUN_TIME

    if (m_listImsNtfToread.empty() && m_pCfg->m_cfgCommon.m_nSleep > 0)
    {
        if (m_isNeedUpdateStatus)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfToreadList, MAbmInterfacePromptDb::CImsNtfToread>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfToread is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_toread order : %d orders from table %s.", m_listImsNtfToread.size(), m_sourceTable.c_str());
    int32 iCount = 0;
    for (m_itrImsNtfToread = m_listImsNtfToread.begin(); m_itrImsNtfToread != m_listImsNtfToread.end(); m_itrImsNtfToread++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfToread = *m_itrImsNtfToread;
        m_actionId = m_itrImsNtfToread->get_actionId();
        m_listImsNtfToreadHis.clear();

        /*
            m_notifCont.init();
            m_notifCont.cust_id = atol64(m_itrImsNtfToread->get_extend3().c_str());
            m_notifCont.controlType = "0";
            m_notifCont.assignStaffid = "SUPERUSR";
            m_notifCont.assignDepartid = "23061";
        */

        int32 retCode = ABMPROMPT_OK;
        if (m_imsNtfToread.get_notifContent().empty())
        {
            retCode = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
            cErrorMsg.set_hint("notif_content is empty");
        }

        //判断该批次是否已经调用过待阅接口
        bool isExistFlag = false;
        if (ABMPROMPT_OK == retCode)
        {
            retCode = checkSeriesId(pSession, m_imsNtfToread, cErrorMsg);
        }

        if (ABMPROMPT_OK == retCode) //查询的结果为空，未调用过待阅,需要调用待阅接口
        {
            retCode = get_actionExtInfo(pSession, cErrorMsg);
            if (retCode != ABMPROMPT_OK)
            {
                AISTD string strErrMsg("action_id = ");
                strErrMsg.append(cdk::strings::Itoa(m_actionId));
                strErrMsg.append("  get action_id info failed!");
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
                cErrorMsg.set_hint(strErrMsg);
                retCode = ABMPROMPT_ERROR;
                LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
            }

            if (ABMPROMPT_OK == retCode)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> call crm toread data : eparchy_code= %d,queue_id=%lld,acct_id = %lld",
                          m_imsNtfToread.get_regionCode(),
                          m_imsNtfToread.get_queueId(),
                          m_imsNtfToread.get_acctId());
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->call_crmService")
                retCode = call_crmService(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
        }
        else if (ABMPROMPT_DONE == retCode) //该批次已经调用成功过
        {
            isExistFlag = true;
            LOG_TRACE(" == [ABM_SERV_ABMPROMPT] == > find batch_id = %lld in Ti_O_CreditWork_Cust , the queue_id = %lld",
                      m_imsNtfToread.get_seriesId(),
                      m_imsNtfToread.get_queueId());
        }

        m_dtDateTime = CBSDateTime::currentDateTime();
        if (ABMPROMPT_OK == retCode || ABMPROMPT_DONE == retCode)
        {

            //待阅工单处理完毕，不需要插入TI_O_CREDITWORK_CUST表
            /*
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_confirm->insert_cust")
            MAbmInterfacePromptDb::CTiOCreditworkCust sCreditWorkCust;
            trans_confirm(m_imsNtfToread, m_notifCont, m_dtDateTime, sCreditWorkCust);
            LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> insert TI_O_CREDITWORK_CUST as follow: %s",
                sCreditWorkCust.to_string().c_str())
            retCode = insertCreditWorkCust(pSession, sCreditWorkCust, cErrorMsg);
            ES_END_RUN_TIME
            */
        }

        if (m_isNeedUpdateStatus &&
            cErrorMsg.get_errorMsg() != "-100" &&
            cErrorMsg.get_errorMsg() != "-101" &&
            cErrorMsg.get_errorMsg() != "-102" &&
            cErrorMsg.get_errorMsg() != "-103")
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfToreadList, MAbmInterfacePromptDb::CImsNtfToread>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_toread->delete_inserthis")
        if (ABMPROMPT_OK != retCode)
        {
            //contrl_rollback(pSession, cErrorMsg);
            if (ABMPROMPT_DB_OP_ERR == retCode)
            {
                m_imsNtfToread.set_status(ABM_PROMPT_PROCESS_CUSTFAILED); // 操作cust工单表失败
            }
            else
            {
                m_imsNtfToread.set_status(ABM_PROMPT_PROCESS_FAILED); // 调CRM待阅接口失败
                if (cErrorMsg.get_errorMsg() == "-100" ||
                    cErrorMsg.get_errorMsg() == "-101" ||
                    cErrorMsg.get_errorMsg() == "-102" ||
                    cErrorMsg.get_errorMsg() == "-103")
                {
                    if (m_imsNtfToread.get_stateDtlId() < STS_START_STATE + m_iUpdateCrmExcep)
                    {
                        m_imsNtfToread.set_status(atoi(cErrorMsg.get_errorMsg().c_str()));
                        m_imsNtfToread.set_stateDtlId(m_imsNtfToread.get_stateDtlId() + 1);
                        m_isNeedUpdateStatus = true;
                    }
                    else
                    {
                        m_imsNtfToread.set_status(ABM_PROMPT_PROCESS_FAILED);
                        m_imsNtfToread.set_stateDtlId(OTHER_START_STATE);
                    }
                }
            }

            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfToread.set_remark(strRemark.substr(0, 1024));
            m_imsNtfToread.set_soDate(m_dtDateTime);

            update_data<MAbmInterfacePromptDb::CImsNtfToreadList>(pSession, cErrorMsg, m_imsNtfToread);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table,%s : %s.",
                      m_imsNtfToread.get_queueId(),
                      m_imsNtfToread.get_acctId(),
                      cErrorMsg.get_errorMsg().c_str(),
                      cErrorMsg.get_hint().c_str());
            //contrl_commit(pSession,cErrorMsg);
            //continue;
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfToreadHis imsNtfToreadHis;
            transToHis(*m_itrImsNtfToread, imsNtfToreadHis);
            imsNtfToreadHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
            if (isExistFlag) //该批次已经执行过调用代阅接口
            {
                imsNtfToreadHis.set_remark("This batch has called ESOP successfully,so this record don't need to call ESOP again.");
            }
            else
            {
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                imsNtfToreadHis.set_remark(strRemark.substr(0, 1024));
            }

            imsNtfToreadHis.set_soDate(m_dtDateTime);
            m_listImsNtfToreadHis.push_back(imsNtfToreadHis);

            if (insert_dataToHisToread(pSession, cErrorMsg) == ABMPROMPT_OK)
            {
                if (delete_data<MAbmInterfacePromptDb::CImsNtfToreadList>(cErrorMsg, m_imsNtfToread) != ABMPROMPT_OK)
                {
                    //contrl_rollback(pSession, cErrorMsg);
                    m_imsNtfToread.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED); // -1
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfToread.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfToread.set_soDate(m_dtDateTime);
                    update_data<MAbmInterfacePromptDb::CImsNtfToreadList>(pSession, cErrorMsg, m_imsNtfToread);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table,%s : %s.",
                              m_imsNtfToread.get_queueId(),
                              m_imsNtfToread.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                    //contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Confirm sub_table, do successfully",
                              m_imsNtfToread.get_queueId(),
                              m_imsNtfToread.get_acctId(),
                              cErrorMsg.get_errorMsg().c_str(),
                              cErrorMsg.get_hint().c_str());
                }
            }
            else
            {
                m_imsNtfToread.set_status(ABM_PROMPT_PROCESS_MOVEFAILED); // 4
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfToread.set_remark(strRemark.substr(0, 1024));
                m_imsNtfToread.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfToreadList>(pSession, cErrorMsg, m_imsNtfToread);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from Toread sub_table,%s : %s.",
                          m_imsNtfToread.get_queueId(),
                          m_imsNtfToread.get_acctId(),
                          cErrorMsg.get_errorMsg().c_str(),
                          cErrorMsg.get_hint().c_str());
            }
        }
        ES_END_RUN_TIME
        if (++iCount % m_pCfg->m_cfgCommon.m_iCommitNum == 0)
        {
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    contrl_commit(pSession, cErrorMsg);
    LEAVE_FUNC
    return ABMPROMPT_OK;
}

int32 ABMPrompt::checkSeriesId(SOBSession *pSession, const MAbmInterfacePromptDb::CImsNtfToread &sImsNtf, CBSErrorMsg &cErrorMsg)
{
    try
    {
        ob_kernel<MAbmInterfacePromptDb::CTiOCreditworkCustList> query("UCR_ACT42");
        MAbmInterfacePromptDb::CTiOCreditworkCustList outList;
        ob_whereCond condition;
        condition << OCS(" BATCH_ID = ") << sImsNtf.get_seriesId();
        query.RemovePartitionKey("REGION_CODE");
        query.AddPartitionKey("REGION_CODE", cdk::strings::Itoa(sImsNtf.get_regionCode()));
        query.select_allExt(outList, condition, NULL, true);
        query.GetNextData(outList, 1);
        if (outList.empty())
        {
            return ABMPROMPT_OK;
        }
    }
    catch (err_info_service::CAIException &e)
    {
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
        cErrorMsg.set_hint(e.get_message());
        LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , catch exception: Code: %lld, message: %s, logrecord: %s",
                  __func__, e.get_code(), e.get_message().c_str(), e.get_logrecord().c_str());
        return ABMPROMPT_DB_OP_ERR;
    }
    catch (otl_exception &e)
    {

        AISTD string strOtl = AISTD string("error_msg=\notl_exception:") + AISTD string((char *)e.msg) + AISTD string(e.stm_text) + AISTD string(e.var_info);
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
        cErrorMsg.set_hint(strOtl);
        LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==>func:%s , Exception: %s", __func__, strOtl.c_str());
        return ABMPROMPT_DB_OP_ERR;
    }
    catch (...)
    {
        cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_DB_OP_ERR));
        cErrorMsg.set_hint("query Ti_O_CreditWork_Cust unknown exception ");
        LOG_ERROR(-1, "==[ABM_SERV_ABMPROMPT]==> func:%s , catch unknown exception!!!========", __func__);
        return ABMPROMPT_DB_OP_ERR;
    }
    return ABMPROMPT_DONE;
}

bool ABMPrompt::InSysParamProd(const AISTD string &strParam, int32 lProductId )
{
    AISTD string strtmpValue;
    get_sysParamter(strParam, strtmpValue);
    CStringList productIDList;
    cdk::strings::Split(strtmpValue, "|", productIDList);
    map<int64,int64> mapProduct;
    for (CStringList::iterator it = productIDList.begin(); it != productIDList.end(); ++it)
    {
        int32 lProcuetIdT = atoi((*it).c_str());
        if( lProductId == lProcuetIdT )
        {
            return true;
        }
    }
    return false;
}

bool ABMPrompt::judgeIsMergeProduct(int32 llProductId, int32 iRegionCode)
{
    aistring strtmpValue(""); //
    get_sysParamter("MERGEPRODUCT_TAG", strtmpValue);
    if( "1" == strtmpValue )
    {//
        return InSysParamProd("MERGEPRODUCT",llProductId);
    }
    return true;
}

bool ABMPrompt::judgeIsShow(int64 llProductId, int32 iRegionCode)
{
	TRY_BEGIN
	bool bIsFind = false;
	int32 iShowFlag = 0;
	xc::CSnapshot cSnap("ZW::ABM_BALANCE");
	xc::CQueryHolder<ZW::ABM_BALANCE::CNotificationSysNotifyGroup::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotificationSysNotifyGroup::GetContainerName()); 
	ZW::ABM_BALANCE::CNotifyRegionProductKey key(iRegionCode,llProductId);
	ZW::ABM_BALANCE::CNotificationSysNotifyGroup::Type::iterator iter = cQueryHolder.GetContainer().find(ZW::ABM_BALANCE::CNotificationSysNotifyGroup::NOTIFY_REGION_PRODUCT_INDEX_ID,key);
	if(!iter.eof())
	{
		for(; !iter.eof(); ++iter)
		{
			// 该表的生失效时间目前无意义，不需要进行判断
			//if(iter.value().GetValidDate() <= llCurrentTime && iter.value().GetExpireDate() > llCurrentTime)
			{
				iShowFlag = iter.value().GetShowFlag();
				LOG_TRACE("\n===[ABM_SERV_ABMPROMPT]==> product_id:%d find show_flag:%d ",llProductId, iShowFlag);
				bIsFind = true;
				break;
			}
		}
	}
	if(!bIsFind)
	{// 默认地州没有找到配置，采用通配方式寻找
		xc::CSnapshot cSnap1("ZW::ABM_BALANCE");
		xc::CQueryHolder<ZW::ABM_BALANCE::CNotificationSysNotifyGroup::Type> cQueryHolder1(cSnap1, ZW::ABM_BALANCE::CNotificationSysNotifyGroup::GetContainerName()); 
		ZW::ABM_BALANCE::CNotifyRegionProductKey key1(-1,llProductId);
		ZW::ABM_BALANCE::CNotificationSysNotifyGroup::Type::iterator iter1 = cQueryHolder1.GetContainer().find(ZW::ABM_BALANCE::CNotificationSysNotifyGroup::NOTIFY_REGION_PRODUCT_INDEX_ID,key1);
		if(!iter1.eof())
		{
			for(; !iter1.eof(); ++iter1)
			{
				// 该表的生失效时间目前无意义，不需要进行判断
				//if(iter1.value().GetValidDate() <= llCurrentTime && iter1.value().GetExpireDate() > llCurrentTime)
				{
					iShowFlag = iter1.value().GetShowFlag();
					LOG_TRACE("\n===[ABM_SERV_ABMPROMPT]==> redion:-1 product_id:%ld find show_flag:%d ",llProductId, iShowFlag);
					break;
				}
			}
		}
	}
	LOG_TRACE("\n===[ABM_SERV_ABMPROMPT]==> product_id:%ld show_flag:%d ",llProductId, iShowFlag);
	if(1 == iShowFlag)
	{
		return true;
	}
	CATCH_END
	return false;
}

int32 ABMPrompt::getTradeTypeCode(int32 iActionId, int32 &iTradeTypeCode)
{
	ENTER_FUNC
	TRY_BEGIN
	iTradeTypeCode = 0;
	if(ABM_PROMPT_APP_TYPE_MERGECREDIT == m_strAppType || ABM_PROMPT_APP_TYPE_MERGESTOP == m_strAppType 
	  || ABM_PROMPT_APP_TYPE_MERGEWARN == m_strAppType || ABM_PROMPT_APP_TYPE_GRPCREDIT == m_strAppType)
	{
        //获取短信类action附加信息
        xc::CSnapshot cSnap("ZW::ABM_BALANCE");
        xc::CQueryHolder<ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtsms::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtsms::GetContainerName());
        m_sysNotificationActionExtsmsIter = cQueryHolder.GetContainer().find(iActionId);
        if (m_sysNotificationActionExtsmsIter == cQueryHolder.GetContainer().end())
        {
			LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>  can't lookup action_id = %d  from NOTIFYDEAL_SYS_NOTIFICATION_ACTION_EXTSMS", iActionId);
			iTradeTypeCode = 0;
            return ABMPROMPT_OK;
        }
        iTradeTypeCode = m_sysNotificationActionExtsmsIter->second.GetTradeTypeCode();
    }
    else if(ABM_PROMPT_APP_TYPE_GRPSTS == m_strAppType)
    {
        //获取调crm接口类附加信息
        xc::CSnapshot cSnap("ZW::ABM_BALANCE");
        xc::CQueryHolder<ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtcrm::Type> cQueryHolder(cSnap, ZW::ABM_BALANCE::CNotifydealSysNotificationActionExtcrm::GetContainerName());
        m_sysNotificationActionExtcrmIter = cQueryHolder.GetContainer().find(iActionId);
        if (m_sysNotificationActionExtcrmIter == cQueryHolder.GetContainer().end())
        {
			LOG_ERROR(-1, "\n====[ABM_SERV_ABMPROMPT]===>  can't lookup action_id = %d  from NOTIFYDEAL_SYS_NOTIFICATION_ACTION_EXTCRM", iActionId);
			iTradeTypeCode = 0;
            return ABMPROMPT_OK;
        }
        iTradeTypeCode = m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode();
    }
    CATCH_END
	LEAVE_FUNC
	return ABMPROMPT_OK;
}

int32 ABMPrompt::splitWithStr(const AISTD string &in_str, const AISTD string &in_divideStr, vector<string> &out_list)
{
	AISTD string::size_type pos = 0,size = 0, i = 0;
	if (!out_list.empty())
	{
		out_list.clear();
	}
	if(in_str.empty())
	{
		return 0;
	}
	if(in_divideStr.empty())
	{
		out_list.push_back(in_str);
		return 1;
	}
	// 方便截取最后一段
	AISTD string strs = in_str + in_divideStr;
	pos = strs.find(in_divideStr);
	size = strs.size();
	while(pos != AISTD string::npos)
	{
		AISTD string xStr = strs.substr(0,pos);
		if((!xStr.empty()) && (xStr.length()>0))
		{
			out_list.push_back(xStr);
		}
		size = strs.size();
		strs = strs.substr(pos + in_divideStr.size(),size);
		pos = strs.find(in_divideStr);
	}
	return out_list.size();
}

// add by taocj  政企工单独立
// 政企催缴工单
int32 ABMPrompt::process_grpcredit(SOBSession* pSession, CBSErrorMsg& cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit->query_data")
    query_data(m_listImsNtfGrpcredit);
    ES_END_RUN_TIME

    if ( m_listImsNtfGrpcredit.empty() && m_pCfg->m_cfgCommon.m_nSleep>0)
    {
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfGrpcredit is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_grpcredit order : %d orders from table %s.", m_listImsNtfGrpcredit.size(), m_sourceTable.c_str());

    aistring strtmpValue("");       //add by fsl@20191115 for 用户全生命周期信控短信提醒
    CStringList triggeringNotificationIDList;
    get_sysParamter("USER_OWE_FEE_FLAG",strtmpValue);
    cdk::strings::Split(strtmpValue, "|", triggeringNotificationIDList);

    m_listImsNtfGrpcreditHis.clear();
    int32 iCount = 0;
    m_listStatIn.clear();
    m_listStatUp.clear();
    for(m_itrImsNtfGrpcredit=m_listImsNtfGrpcredit.begin(); m_itrImsNtfGrpcredit!=m_listImsNtfGrpcredit.end(); m_itrImsNtfGrpcredit++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        m_imsNtfGrpcredit = *m_itrImsNtfGrpcredit;
        m_actionId =m_imsNtfGrpcredit.get_actionId();

        int32 iRetValue = ABMPROMPT_OK;
        m_isSmsMarket = false;
        m_strSmsTag.clear();
        bool bSmsToTable = true;
        bool bIsCallServiceFail = false;
        m_isNeedGroup = true;
        if(get_actionExtInfo(pSession,cErrorMsg) != ABMPROMPT_OK)
        {
            AISTD string strErrMsg("action_id = ");
            strErrMsg.append(cdk::strings::Itoa(m_actionId));
            strErrMsg.append("  get action_id info failed");
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
            cErrorMsg.set_hint(strErrMsg);
            iRetValue =  ABMPROMPT_ERROR;
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
        }
        if (m_imsNtfGrpcredit.get_notifContent().empty())
        {
            iRetValue = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(-1));
            cErrorMsg.set_hint("notif_content is empty");
        }
        else if (!check_phoneId(m_imsNtfGrpcredit.get_phoneId()))
        {
            iRetValue = ABMPROMPT_ORDER_PHONE;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ORDER_PHONE));
            cErrorMsg.set_hint("phone_id is inaccurate ,don't need to send message");
        }
        else if (!check_notifyContent(m_imsNtfGrpcredit.get_notifContent()))
        {
            iRetValue = ABMPROMPT_ERROR;
            cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_ERROR_MESSAGE));
            cErrorMsg.set_hint("the notif_content contains  '<' or '>' ,some param_id don't have be replaced.");
        }

		if (!m_imsNtfGrpcredit.get_remark().empty())
		{// 判断下是否已入明细表 add by ligc@20200720
			vector<string> vct_remark;
			cdk::strings::Split(m_imsNtfGrpcredit.get_remark(), "|", vct_remark);
			if (vct_remark.size() == 2)
			{
				if(vct_remark[1] == "ok")
				{
					m_isNeedGroup = false;
				}
			}
		}
		
        if (ABMPROMPT_OK==iRetValue)
        {

            m_listTiOSms.Clear();
            MAbmInterfacePromptDb::CTiOSms tiOSmsTmp;
            CBSDateTime dealTime(2050, 12, 31, 23, 59, 59);
            char eparchyCode[5];
            sprintf(eparchyCode, "%04d", m_imsNtfGrpcredit.get_regionCode());
            char recv4[17];
            sprintf(recv4, "%d", m_imsNtfGrpcredit.get_amount());
            // mod by ligc@20190418 短信夹带
            if ((!m_crmHttpServiceName.empty())&&(m_isSmsMarket)&&(m_isCallSmsService))
            {
                bSmsToTable = false;
            }
            if(m_strSmsTag!="0")//0表示只调消息中心，不发送短信
            {
                /*tiOSmsTmp.set_smsNoticeId(TI_O_SMS_SEQUECE_BASE + m_imsNtfGrpcredit.get_queueId());
                tiOSmsTmp.set_eparchyCode(eparchyCode);
                tiOSmsTmp.set_inModeCode("0");
                tiOSmsTmp.set_smsChannelCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
                tiOSmsTmp.set_sendObjectCode(2);
                tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                tiOSmsTmp.set_sendCountCode(1);
                tiOSmsTmp.set_recvObjectType("00");
                tiOSmsTmp.set_recvObject(m_imsNtfGrpcredit.get_phoneId());
                tiOSmsTmp.set_id(m_imsNtfGrpcredit.get_resourceId());
                tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                tiOSmsTmp.set_smsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                tiOSmsTmp.set_noticeContentType("0");
                tiOSmsTmp.set_noticeContent(m_imsNtfGrpcredit.get_notifContent());
                //tiOSmsTmp.set_referedCount(const int32 & value);
                tiOSmsTmp.set_forceReferCount(1);
                //tiOSmsTmp.set_forceObject(const aistring & value);
                //tiOSmsTmp.set_forceStartTime(const CBSDateTime & value);
                //tiOSmsTmp.set_forceEndTime(const CBSDateTime & value);
                tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                tiOSmsTmp.set_referTime(m_dtDateTime);
                tiOSmsTmp.set_referStaffId("CREDIT00");
                tiOSmsTmp.set_referDepartId("CREDI");
                tiOSmsTmp.set_dealTime(dealTime);
                //tiOSmsTmp.set_dealStaffid(const aistring & value);
                //tiOSmsTmp.set_dealDepartid(const aistring & value);
                tiOSmsTmp.set_dealState("0");
                //tiOSmsTmp.set_remark(const aistring & value);
                //tiOSmsTmp.set_revc1(const aistring & value);
                //tiOSmsTmp.set_revc2(const aistring & value);
                //tiOSmsTmp.set_revc3(const aistring & value);
                tiOSmsTmp.set_revc4(recv4); //结余值
                tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));

                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms  data : eparchy_code= %s,queue_id=%lld,acct_id = %lld",
                tiOSmsTmp.get_eparchyCode().c_str(),
                m_imsNtfGrpcredit.get_queueId(),
                m_imsNtfGrpcredit.get_acctId());*/
            
            //add by xupp for yunnanV8 begin

            tiOSmsTmp.set_tradeId( TI_O_SMS_SEQUECE_BASE + m_imsNtfGrpcredit.get_queueId() );
            //tiOSmsTmp.set_tradeId(strQueueId);
            int16 iPartitionId = tiOSmsTmp.get_tradeId() % TI_O_SMS_PARTITION_MOD;
            tiOSmsTmp.set_partitionId(iPartitionId);
            
            tiOSmsTmp.set_sourceCode(m_sysNotificationActionExtsmsIter->second.GetSmsChannelCode());
            tiOSmsTmp.set_eparchyCode(eparchyCode);

                AISTD string strSmsKindCode(m_sysNotificationActionExtsmsIter->second.GetSmsKindCode());
                tiOSmsTmp.set_inModeCode( strSmsKindCode.substr(0,1) );
                tiOSmsTmp.set_sendObjectCode("01");

                tiOSmsTmp.set_sendTimeCode(m_sysNotificationActionExtsmsIter->second.GetSendTimeCode());
                tiOSmsTmp.set_smsTypeCode(m_sysNotificationActionExtsmsIter->second.GetSmsTypeCode());
                tiOSmsTmp.set_receveObject(m_imsNtfGrpcredit.get_phoneId());
                tiOSmsTmp.set_sendObject("");
                tiOSmsTmp.set_smsContent(m_imsNtfGrpcredit.get_notifContent());
                tiOSmsTmp.set_referTime(m_dtDateTime);

                tiOSmsTmp.set_smsPriority(m_sysNotificationActionExtsmsIter->second.GetSmsPriority());
                tiOSmsTmp.set_referStaffId("CREDIT00");
                tiOSmsTmp.set_referDepartId("CREDI");


                tiOSmsTmp.set_month(atoi(m_dtDateTime.toString("%M").c_str()));
                tiOSmsTmp.set_day(atoi(m_dtDateTime.toString("%D").c_str()));

                AISTD string strQueueIdT = ltoa(m_imsNtfGrpcredit.get_queueId());
                tiOSmsTmp.set_rsrvStr4(strQueueIdT);//SINGLE
                LOG_TRACE( "\n==[ABM_SERV_ABMPROMPT]==> print ti_o_sms  data : eparchy_code= %s,queue_id=%lld",
                          tiOSmsTmp.get_eparchyCode().c_str(),
                          tiOSmsTmp.get_tradeId() );

                m_listTiOSms.push_back(tiOSmsTmp);
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit->insert_to_sms")
                iRetValue = insertIntoSms(pSession, cErrorMsg);
                ES_END_RUN_TIME

            }
            if((bSmsToTable==false))//配置了调用消息中心服务,需要调用消息中心
            { // 调用服务
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit->call_crmService")
                iRetValue = call_crmService(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
        }

        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit->delete_inserthis")
        if(iRetValue != ABMPROMPT_OK && ABMPROMPT_ORDER_PHONE != iRetValue)
        {
            //contrl_rollback(pSession, cErrorMsg);
            m_imsNtfGrpcredit.set_status(ABM_PROMPT_PROCESS_FAILED);
            AISTD string strRemark(cErrorMsg.get_errorMsg());
            strRemark.append(":");
            strRemark.append(cErrorMsg.get_hint());
            m_imsNtfGrpcredit.set_remark(strRemark.substr(0,2048));
            m_imsNtfGrpcredit.set_soDate(m_dtDateTime);
            update_data<MAbmInterfacePromptDb::CImsNtfGrpcreditList>(pSession, cErrorMsg, m_imsNtfGrpcredit);
            LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Grp Credit sub table, %s : %s return to ims_ntf_grpcredit",
                m_imsNtfGrpcredit.get_queueId(),
                m_imsNtfGrpcredit.get_acctId(),
                cErrorMsg.get_errorMsg().c_str(),
                cErrorMsg.get_hint().c_str());
        }
        else
        {
            MAbmInterfacePromptDb::CImsNtfGrpcreditHis  imsNtfGrpcreditHis;
            MAbmInterfacePromptDb::CImsNtfGroupHis  imsNtfGroupHis;
            transToHis(m_imsNtfGrpcredit, imsNtfGrpcreditHis);
            if (iRetValue == ABMPROMPT_OK)
            {
                imsNtfGrpcreditHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                // mod by ligc@20190505 短信夹带
                if (bSmsToTable && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel && ENUM_ACTION_LEVEL_APP != m_nActionLevel)
                {
                    imsNtfGrpcreditHis.set_remark("insert into ti_o_sms OK");
                }
                else if (bIsCallServiceFail && ENUM_ACTION_LEVEL_WECHAT != m_nActionLevel && ENUM_ACTION_LEVEL_APP != m_nActionLevel)
                {
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    strRemark.append("|insert into ti_o_sms OK");
                    imsNtfGrpcreditHis.set_remark(strRemark.substr(0,2048));
                }
                else if(ENUM_ACTION_LEVEL_WECHAT == m_nActionLevel)
                {
                    imsNtfGrpcreditHis.set_remark("insert into ti_o_fsbd_wxmsg OK");
                }
                else if(ENUM_ACTION_LEVEL_APP == m_nActionLevel)
                {
                    imsNtfGrpcreditHis.set_remark("insert into ti_o_fsbd_appmsg OK");
                }
                else
                {
                    imsNtfGrpcreditHis.set_remark((cErrorMsg.get_errorMsg()+":"+cErrorMsg.get_hint()).substr(0,2048));
                }
                // end mod
            }
            else
            {
                imsNtfGrpcreditHis.set_status(ABM_PROMPT_PROCESS_PHONE_FILTER);
                imsNtfGrpcreditHis.set_remark((cErrorMsg.get_errorCode()+":"+cErrorMsg.get_hint()).substr(0,2048));
            }

            imsNtfGrpcreditHis.set_soDate(m_dtDateTime);
            m_listImsNtfGrpcreditHis.push_back(imsNtfGrpcreditHis);

            iRetValue = insert_dataToHisGrpcredit(pSession, cErrorMsg);
            m_listImsNtfGrpcreditHis.clear();

			// add by ligc@20200720 判断是否需要入明细表
			if (ABMPROMPT_OK==iRetValue)
			{
				if (m_isNeedGroup)
				{
					m_listImsNtfGroupHis.clear();
					if(imsNtfGrpcreditHis.get_productId() > 0)
					{
						int64 llProductId = imsNtfGrpcreditHis.get_productId();
						if(judgeIsShow(llProductId, imsNtfGrpcreditHis.get_regionCode()))
						{// 需要入明细表，则获取相关数据
							tran2GroupHis(imsNtfGrpcreditHis, imsNtfGroupHis);
							int32 iTradeTypeCode = 0;
							if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
							{
								int64 llCust_id = 0;
								int32 lCountyCode = 0;
								getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCust_id, lCountyCode);
								imsNtfGroupHis.set_srcType("CREDIT");
								imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
								imsNtfGroupHis.set_custId(llCust_id);
								imsNtfGroupHis.set_assetAmount(0);
								imsNtfGroupHis.set_productId(llProductId);
								imsNtfGroupHis.set_updateTime(m_dtDateTime);
								imsNtfGroupHis.set_countyCode(lCountyCode);
								m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
								LOG_TRACE("queue_id:%ld, acct_id:%lld,cust_id:%lld need into group!",imsNtfGroupHis.get_queueId(),imsNtfGroupHis.get_acctId(),llCust_id);
							}
						}
						else
						{
							m_isNeedGroup = false; // 不在配置表中，也不入明细表
						}
					}
					else
					{
						m_isNeedGroup = false; // 产品ID不正确，也不入明细表
					}
				}
				if (m_isNeedGroup)
				{
					m_isNeedGroup = insert_dataToHisGroup(pSession, cErrorMsg);
					m_listImsNtfGroupHis.clear();
					
				}
			}
			// end add

            if (ABMPROMPT_OK==iRetValue)
            {            
                iRetValue = delete_data<MAbmInterfacePromptDb::CImsNtfGrpcreditList>(cErrorMsg, m_imsNtfGrpcredit);
                if (iRetValue!=ABMPROMPT_OK)
                {
                    contrl_rollback(pSession, cErrorMsg);
                    m_listStatUp.clear();
                    m_listStatIn.clear();
                    m_imsNtfGrpcredit.set_status(ABM_PROMPT_PROCESS_REMOVEFAILED);
                    AISTD string strRemark(cErrorMsg.get_errorMsg());
                    strRemark.append(":");
                    strRemark.append(cErrorMsg.get_hint());
                    m_imsNtfGrpcredit.set_remark(strRemark.substr(0, 1024));
                    m_imsNtfGrpcredit.set_soDate(m_dtDateTime);
                    update_data<MAbmInterfacePromptDb::CImsNtfGrpcreditList>(pSession, cErrorMsg, m_imsNtfGrpcredit);
                    LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Grpcredit sub table, %s : %s return to ims_ntf_grpcredit",
                    m_imsNtfGrpcredit.get_queueId(),
                    m_imsNtfGrpcredit.get_acctId(),
                    cErrorMsg.get_errorMsg().c_str(),
                    cErrorMsg.get_hint().c_str());
                    contrl_commit(pSession, cErrorMsg);
                }
                else
                {
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpcredit->statImsNtf")
                    statImsNtf(pSession, cErrorMsg, m_sysNotificationActionExtsmsIter->second.GetTradeTypeCode(), m_imsNtfGrpcredit);
                    ES_END_RUN_TIME
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld , acct_id = %lld come from grpcredit sub_table is done successed",
                        m_imsNtfGrpcredit.get_queueId(),
                        m_imsNtfGrpcredit.get_acctId(),
                        cErrorMsg.get_errorMsg().c_str(),
                        cErrorMsg.get_hint().c_str());
                }
            }
            else
            {
                contrl_rollback(pSession, cErrorMsg);
                m_listStatIn.clear();
                m_listStatUp.clear();
                m_imsNtfGrpcredit.set_status(ABM_PROMPT_PROCESS_MOVEFAILED);
                AISTD string strRemark(cErrorMsg.get_errorMsg());
                strRemark.append(":");
                strRemark.append(cErrorMsg.get_hint());
                m_imsNtfGrpcredit.set_remark(strRemark.substr(0, 1024));
                m_imsNtfGrpcredit.set_soDate(m_dtDateTime);
                update_data<MAbmInterfacePromptDb::CImsNtfGrpcreditList>(pSession, cErrorMsg, m_imsNtfGrpcredit);
                contrl_commit(pSession, cErrorMsg);
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> queue_id =%lld ,acct_id = %lld come from Credit sub table, %s : %s return to ims_ntf_grpcredit",
                    m_imsNtfGrpcredit.get_queueId(),
                    m_imsNtfGrpcredit.get_acctId(),
                    cErrorMsg.get_errorMsg().c_str(),
                    cErrorMsg.get_hint().c_str());
            }
        }

        if (++iCount%m_pCfg->m_cfgCommon.m_iCommitNum==0)
        {
            upStatPrompt(pSession, cErrorMsg);
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }
        ES_END_RUN_TIME

        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    upStatPrompt(pSession, cErrorMsg);
    contrl_commit(pSession, cErrorMsg);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> loop process success : %d orders processed. ", m_listImsNtfGrpcredit.size());
    LEAVE_FUNC
    return ABMPROMPT_OK;
}
// 政企停机工单
int32 ABMPrompt::process_grpsts(SOBSession* pSession, CBSErrorMsg& cErrorMsg)
{
    ENTER_FUNC

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->query_data")
    query_data(m_listImsNtfGrpsts);
    ES_END_RUN_TIME

    if (m_listImsNtfGrpsts.empty() && m_pCfg->m_cfgCommon.m_nSleep>0)
    {
        if (m_isNeedUpdateStatus)
        {
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
            update_status<MAbmInterfacePromptDb::CImsNtfGrpstsList, MAbmInterfacePromptDb::CImsNtfGrpsts>(pSession, cErrorMsg);
            ES_END_RUN_TIME
        }
        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> m_listImsNtfGrpsts is empty, sleep  %d s", m_pCfg->m_cfgCommon.m_nSleep);
        sleep(m_pCfg->m_cfgCommon.m_nSleep);
        return ABMPROMPT_OK;
    }
    //char buff[31];
    //snprintf(buff, sizeof(buff), "%s%d", m_sourceTable.c_str(), m_pCfg->m_cfgParams.m_nTablePartNo);
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_gprsts order : %d orders from table %s.", m_listImsNtfGrpsts.size(), m_sourceTable.c_str());

    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->repeat_filter")
    repeat_filter(m_listImsNtfGrpsts.begin(), m_listImsNtfGrpsts.end(),m_userList);
    ES_END_RUN_TIME
    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> query ims_ntf_sts order user count =  %d  from table %s.", m_userList.size(), m_sourceTable.c_str());

    m_listImsNtfSts.clear();
    int32 iCount = 0;
    m_listStatIn.clear();
    m_listStatUp.clear();
    for( m_itrUserList=m_userList.begin(); m_itrUserList!=m_userList.end();m_itrUserList++)
    {
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->do_for")
        m_dtDateTime = CBSDateTime::currentDateTime();
        int32 retCode = ABMPROMPT_OK;
        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->get_dataSts")
        retCode= get_dataGrpsts(pSession, cErrorMsg);
        ES_END_RUN_TIME

        //停开机工单历史表初始化
        m_listImsNtfGrpstsHis.clear();
        //停开机错单表初始化
        m_listImsNtfGrpstsErrHis.clear();
        //明细表初始化
        m_listImsNtfGroupHis.clear();

        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> serv_id = %lld 's orders size  = %d ...", *m_itrUserList, m_listImsNtfGrpsts.size());
        if(m_listImsNtfGrpsts.empty())
        {
            continue;
        }
        m_imsNtfGrpsts = m_listImsNtfGrpsts[0];
        m_isNeedGroup = true;

        iCount += m_listImsNtfGrpsts.size();
        bool isProc = true;
        //当前进程是开机进程。取第一条最新的工单，若最新工单不是开机类工单，直接跳过，让停机进程处理。
        if(STS_TYPE_OPEN==m_pCfg->m_cfgParams.m_stsType)
        {
            if(m_imsNtfGrpsts.get_stateId()!=m_pCfg->m_cfgCommon.m_openStateId)
            {
                //Todo:需要处理前面开的工单：开-》停=停
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> serv_id = %lld, last state_id = %d, but the proc sts_type = %d.",
                    *m_itrUserList,
                    m_imsNtfGrpsts.get_stateId(),
                    m_pCfg->m_cfgParams.m_stsType);
                isProc = false;
                //continue;
            }
        }
        else //当前进程是停机进程。取第一条最新的工单，若最新工单是开机类工单，直接跳过，让开机进程处理。
        {
            if(m_imsNtfGrpsts.get_stateId()==m_pCfg->m_cfgCommon.m_openStateId)
            {
                //Todo:需要处理前面停的工单：停-》开=开
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> serv_id = %lld, last state_id = %d, but the proc sts_type = %d.",
                    *m_itrUserList,
                    m_imsNtfGrpsts.get_stateId(),
                    m_pCfg->m_cfgParams.m_stsType);
                isProc = false;
                //continue;
            }
        }
        bool bInProduct = false ;
        if( InSysParamProd( "GRPSTOP_NODEAL_PRODUCT",m_imsNtfGrpsts.get_productId() ) )
        {
            isProc = false;
            bInProduct = true;
        }
        //如果不是当前处理工单，直接把超时工单移到历史表中
        //qiankun3 20190304 信控错单优化，
        //将超时工单入错单表
        if (!isProc)
        {
        	int64 llOldAcctId = 0;
        	int64 llOldCustId = 0;
        	int32 lOldCountyCode = 0;
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->delete_inserthis_update")
            for (m_itrImsNtfGrpsts = m_listImsNtfGrpsts.begin(); m_itrImsNtfGrpsts != m_listImsNtfGrpsts.end(); m_itrImsNtfGrpsts++)
            {
                int64 llQueueId = 0;
                m_listImsNtfGrpstsErrHis.clear();
                m_listImsNtfGroupHis.clear();
                MAbmInterfacePromptDb::CImsNtfGrpstsHis imsNtfGrpstsHis;
                transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);

                if (m_itrImsNtfGrpsts == m_listImsNtfGrpsts.begin())
                {
                    llQueueId = m_itrImsNtfGrpsts->get_queueId();
                    continue;
                }
                else
                {
                    imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                    if (llQueueId > 0)
                    {
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                    }
                    else
                    {
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. ");
                    }
                }
                if(bInProduct)
                {
                    imsNtfGrpstsHis.set_notifContent("filterd by productId. ");
                }
                imsNtfGrpstsHis.set_soDate(m_dtDateTime);
                m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);

                // add by ligc@20200720 停开机工单入明细表判断
                if (m_isNeedGroup)
                {
                	int64 llProductId = imsNtfGrpstsHis.get_productId();
                	if(judgeIsShow(llProductId, imsNtfGrpstsHis.get_regionCode()))
                	{
	                	MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
						tran2GroupHis(imsNtfGrpstsHis, imsNtfGroupHis);
						int32 iTradeTypeCode = 0;
						if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
						{
	                		int64 llCustId = 0;
	                		int32 lCountyCode = 0;
	                		if(llOldAcctId != imsNtfGroupHis.get_acctId())
	                		{// 减少数据库操作
	                			getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId ,lCountyCode);
	                			llOldCustId = llCustId;
	                			llOldAcctId = imsNtfGroupHis.get_acctId();
	                			lOldCountyCode = lCountyCode;
	                		}
	                		else
	                		{
	                			llCustId = llOldCustId;
	                			lCountyCode = lOldCountyCode;
	                		}
	                		imsNtfGroupHis.set_srcType("STS");
							imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
							imsNtfGroupHis.set_custId(llCustId);
							imsNtfGroupHis.set_assetAmount(imsNtfGrpstsHis.get_openAmount());
							imsNtfGroupHis.set_productId(llProductId);
							imsNtfGroupHis.set_updateTime(m_dtDateTime);
							imsNtfGroupHis.set_countyCode(lCountyCode);
							m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
						}
                	}
                }
                // end add

                if (imsNtfGrpstsHis.get_queueId() != llQueueId)
                {
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.", imsNtfGrpstsHis.get_queueId(), llQueueId);
                }
                grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
            }
            ES_END_RUN_TIME
        }
        else
        {
            m_actionId = m_imsNtfGrpsts.get_actionId();
            //获取接口地址等信息
            if (get_actionExtInfo(pSession, cErrorMsg) != ABMPROMPT_OK)
            {
                AISTD string strErrMsg("action_id = ");
                strErrMsg.append(cdk::strings::Itoa(m_actionId));
                strErrMsg.append(" get action_id info failed!");
                cErrorMsg.set_errorMsg(cdk::strings::Itoa(ABMPROMPT_XC_ERROR));
                cErrorMsg.set_hint(strErrMsg);
                retCode = ABMPROMPT_ERROR;
                LOG_ERROR(-1, "\n==[ABM_SERV_ABMPROMPT]==> %s", strErrMsg.c_str());
            }

            //调用接口，根据接口返回码做不同的处理
            aistring strCallCrmTime("");
            if (retCode == ABMPROMPT_OK)
            {
                LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> call crm to change user's stop/open status data : eparchy_code= %d,queue_id=%lld,acct_id = %lld",
                    m_imsNtfGrpsts.get_regionCode(),
                    m_imsNtfGrpsts.get_queueId(),
                    m_imsNtfGrpsts.get_acctId());
                    m_dtDateTime = CBSDateTime::currentDateTime();
                    strCallCrmTime.append("|");
                    strCallCrmTime.append(m_dtDateTime.toString("%Y%M%D%H%N%S")) ;
                    ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->call_crmService")
                        retCode = call_crmService(pSession, cErrorMsg);
                    ES_END_RUN_TIME
                    strCallCrmTime.append(":");
                    m_dtDateTime = CBSDateTime::currentDateTime();
                    strCallCrmTime.append(m_dtDateTime.toString("%Y%M%D%H%N%S")) ;

            }

            bool dmlTag = true;
            m_dtDateTime = CBSDateTime::currentDateTime();

            //在前面数据有网络数据，且本次操作网络已经连通
            if (m_isNeedUpdateStatus &&
                cErrorMsg.get_errorMsg() != "-100" &&
                cErrorMsg.get_errorMsg() != "-101" &&
                cErrorMsg.get_errorMsg() != "-102" &&
                cErrorMsg.get_errorMsg() != "-103")
            {
                ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->update_status")
                update_status<MAbmInterfacePromptDb::CImsNtfGrpstsList, MAbmInterfacePromptDb::CImsNtfGrpsts>(pSession, cErrorMsg);
                ES_END_RUN_TIME
            }
            ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->delete_inserthis_update")
            //qiankun3 20190304 信控错单优化
            //将超时工单入错单表
            //将处理成功工单进历史表
            if(ABMPROMPT_OK == retCode)
            {
                int64 llQueueId = 0;
                int64 llOldAcctId = 0;
                int64 llOldCustId = 0;
                int32 lOldCountyCode = 0;
                for (m_itrImsNtfGrpsts = m_listImsNtfGrpsts.begin(); m_itrImsNtfGrpsts != m_listImsNtfGrpsts.end(); ++m_itrImsNtfGrpsts)
                {
                    //生成历史表工单
                    m_listImsNtfGrpstsHis.clear();
                    //生成错单表工单
                    m_listImsNtfGrpstsErrHis.clear();
                    m_listImsNtfGroupHis.clear();
                    MAbmInterfacePromptDb::CImsNtfGrpstsHis imsNtfGrpstsHis;
                    MAbmInterfacePromptDb::CImsNtfGrpstserrHis imsNtfGrpstsErrHis;

                    if (m_itrImsNtfGrpsts == m_listImsNtfGrpsts.begin())
                    {
                        transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);
                        llQueueId = m_itrImsNtfGrpsts->get_queueId();

                        imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_SUCCESS);
                        ES_BEGIN_RUN_TIME("ABM_SERV_ABMPROMPT->process_grpsts->statImsNtf")
                        statImsNtf(pSession, cErrorMsg, m_sysNotificationActionExtcrmIter->second.GetTradeTypeCode(), *m_itrImsNtfGrpsts);
                        ES_END_RUN_TIME
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfGrpstsHis.set_notifContent(strRemark.substr(0, 1024));
                        imsNtfGrpstsHis.set_soDate(m_dtDateTime);
                        m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);
                    }
                    else
                    {
                        transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);
                        imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                        if (llQueueId > 0)
                        {
                            imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                        }
                        else
                        {
                            imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. ");
                        }
                        imsNtfGrpstsHis.set_soDate(m_dtDateTime);
                        m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);
                    }

                    // add by ligc@20200720 停开机工单入明细表判断
	                if (m_isNeedGroup)
	                {
	                	int64 llProductId = 0;
	                	AISTD string strExttend1 = "";
	                	MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
	                	if(m_itrImsNtfGrpsts == m_listImsNtfGrpsts.begin())
		                {
		                	tran2GroupHis(imsNtfGrpstsHis, imsNtfGroupHis);
		                	strExttend1 = imsNtfGrpstsHis.get_extend1();
		                }
		                else
		                {
		               		tran2GroupHis(imsNtfGrpstsHis, imsNtfGroupHis);
		               		strExttend1 = imsNtfGrpstsHis.get_extend1();
		                }
	                	llProductId = imsNtfGrpstsHis.get_productId();
	                	if(judgeIsShow(llProductId, imsNtfGroupHis.get_regionCode()))
	                	{
							int32 iTradeTypeCode = 0;
							if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
							{
		                		int64 llCustId = 0;
		                		int32 lCountyCode = 0;
		                		if(llOldAcctId != imsNtfGroupHis.get_acctId())
		                		{// 减少数据库操作
		                			getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
		                			llOldCustId = llCustId;
		                			llOldAcctId = imsNtfGroupHis.get_acctId();
		                			lOldCountyCode = lCountyCode;
		                		}
		                		else
		                		{
		                			llCustId = llOldCustId;
		                			lCountyCode = lOldCountyCode;
		                		}
		                		imsNtfGroupHis.set_srcType("STS");
								imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
								imsNtfGroupHis.set_custId(llCustId);
								imsNtfGroupHis.set_assetAmount(imsNtfGrpstsHis.get_openAmount());
								imsNtfGroupHis.set_productId(llProductId);
								imsNtfGroupHis.set_updateTime(m_dtDateTime);
								imsNtfGroupHis.set_countyCode(lCountyCode);
								m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
							}
	                	}
	                }
	                // end add

                    if (imsNtfGrpstsHis.get_queueId() != llQueueId)
                    {
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.", imsNtfGrpstsHis.get_queueId(), llQueueId);
                    }
                    grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                }
            }
            //qiankun3 20190304 信控错单优化
            //将crm重复工单入错单表
            else if(ABMPROMPT_REPEATED==retCode)
            {
                int64 llQueueId = 0;
                int64 llOldAcctId = 0;
                int64 llOldCustId = 0;
                int32 lOldCountyCode = 0;
                for (m_itrImsNtfGrpsts = m_listImsNtfGrpsts.begin(); m_itrImsNtfGrpsts != m_listImsNtfGrpsts.end(); ++m_itrImsNtfGrpsts)
                {
                    //生成错单表工单
                    m_listImsNtfGrpstsErrHis.clear();
                    m_listImsNtfGroupHis.clear();
                    MAbmInterfacePromptDb::CImsNtfGrpstserrHis imsNtfGrpstsErrHis;
					MAbmInterfacePromptDb::CImsNtfGrpstsHis imsNtfGrpstsHis;
                    
                    if (m_itrImsNtfGrpsts == m_listImsNtfGrpsts.begin())
                    {
						transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsErrHis);
                        llQueueId = m_itrImsNtfGrpsts->get_queueId();
                        imsNtfGrpstsErrHis.set_status(ABM_PROMPT_PROCESS_STS_REPEAT);
                        //qiankun3 20190305 信控工单优化细分错单类型
                        if(CRM_RETURN_CODE_CRM_BOF_002 == cErrorMsg.get_errorMsg())
                        {
                            imsNtfGrpstsErrHis.set_errStatus(ABM_CRM_115003);
                        }
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfGrpstsErrHis.set_notifContent(strRemark.substr(0, 1024));
						imsNtfGrpstsErrHis.set_soDate(m_dtDateTime);
						m_listImsNtfGrpstsErrHis.push_back(imsNtfGrpstsErrHis);
                    }
                    else
                    {
						transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);
                        imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                        if (llQueueId > 0)
                        {
                            imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                        }
                        else
                        {
                            imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. ");
                        }
						imsNtfGrpstsHis.set_soDate(m_dtDateTime);
						m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);
						if (imsNtfGrpstsHis.get_queueId() != llQueueId)
	                    {
	                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.", imsNtfGrpstsHis.get_queueId(), llQueueId);
	                    }
                    }

                    // add by ligc@20200720 停开机工单入明细表判断
	                if (m_isNeedGroup)
	                {
	                	int64 llProductId = m_itrImsNtfGrpsts->get_productId();
	                	if(judgeIsShow(llProductId, m_itrImsNtfGrpsts->get_regionCode()))
	                	{
		                	MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
							tran2GroupHis((*m_itrImsNtfGrpsts), imsNtfGroupHis);
							int32 iTradeTypeCode = 0;
							if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
							{
		                		int64 llCustId = 0;
		                		int32 lCountyCode = 0;
		                		if(llOldAcctId != imsNtfGroupHis.get_acctId())
		                		{// 减少数据库操作
		                			getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
		                			llOldCustId = llCustId;
		                			llOldAcctId = imsNtfGroupHis.get_acctId();
		                			lOldCountyCode = lCountyCode;
		                		}
		                		else
		                		{
		                			llCustId = llOldCustId;
		                			lCountyCode = lOldCountyCode;
		                		}
								imsNtfGroupHis.set_soDate(m_dtDateTime);
		                		imsNtfGroupHis.set_srcType("STS");
								imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
								imsNtfGroupHis.set_custId(llCustId);
								imsNtfGroupHis.set_assetAmount(m_itrImsNtfGrpsts->get_openAmount());
								imsNtfGroupHis.set_productId(llProductId);
								imsNtfGroupHis.set_updateTime(m_dtDateTime);
								imsNtfGroupHis.set_countyCode(lCountyCode);
								m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
							}
	                	}
	                }
	                // end add
	                
                    grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                }

            }
            //qiankun3 20190304 信控错单优化
            //将crm错单工单入错单表
            else if (ABMPROMPT_ERROR == retCode)
            {
                int64 llQueueId = 0;
                int64 llOldAcctId = 0;
                int64 llOldCustId = 0;
                int32 lOldCountyCode = 0;
                for (m_itrImsNtfGrpsts = m_listImsNtfGrpsts.begin(); m_itrImsNtfGrpsts != m_listImsNtfGrpsts.end(); ++m_itrImsNtfGrpsts)
                {
                    //生成历史表工单
                    m_listImsNtfGrpstsErrHis.clear();
                    m_listImsNtfGroupHis.clear();
                    if (m_itrImsNtfGrpsts == m_listImsNtfGrpsts.begin())
                    {
                        MAbmInterfacePromptDb::CImsNtfGrpsts imsNtfGrpsts = *m_itrImsNtfGrpsts;
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfGrpsts.set_status(ABM_PROMPT_PROCESS_FAILED);//处理错误，需要手工处理
                        llQueueId = m_itrImsNtfGrpsts->get_queueId();
                        if (cErrorMsg.get_errorMsg() == "-100"||
                            cErrorMsg.get_errorMsg() == "-101"||
                            cErrorMsg.get_errorMsg() == "-102"||
                            cErrorMsg.get_errorMsg() == "-103")
                        {
                            if (imsNtfGrpsts.get_stateDtlId()<STS_START_STATE+ m_iUpdateCrmExcep)
                            {
                                imsNtfGrpsts.set_status(atoi(cErrorMsg.get_errorMsg().c_str()));
                                imsNtfGrpsts.set_stateDtlId(imsNtfGrpsts.get_stateDtlId() + 1);
                                m_isNeedUpdateStatus = true;
                                imsNtfGrpsts.set_notifContent(strRemark.substr(0, 1024));
                                imsNtfGrpsts.set_soDate(m_dtDateTime);
                                update_data<MAbmInterfacePromptDb::CImsNtfGrpstsList>(pSession, cErrorMsg, imsNtfGrpsts);
                            }
                            else
                            {
                                //恢复值state_dtl_id的值
                                imsNtfGrpsts.set_stateDtlId(STS_START_STATE);
                                imsNtfGrpsts.set_status(ABM_PROMPT_PROCESS_PROC_TRY);
                                //strRemark.append(":网络问题,重做失败!");
                                MAbmInterfacePromptDb::CImsNtfGrpstserrHis imsNtfGrpstsErrHis;
                                transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsErrHis);
                                imsNtfGrpstsErrHis.set_status(100);
                                imsNtfGrpstsErrHis.set_notifContent(strRemark.substr(0, 1024));
                                imsNtfGrpstsErrHis.set_soDate(m_dtDateTime);
                                m_listImsNtfGrpstsErrHis.push_back(imsNtfGrpstsErrHis);
                                
                                // add by ligc@20200720 停开机工单入明细表判断
				                if (m_isNeedGroup)
				                {
				                	int64 llProductId = imsNtfGrpstsErrHis.get_productId();
				                	if(judgeIsShow(llProductId, imsNtfGrpstsErrHis.get_regionCode()))
				                	{
					                	MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
										tran2GroupHis(imsNtfGrpstsErrHis, imsNtfGroupHis);
										int32 iTradeTypeCode = 0;
										if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
										{
					                		int64 llCustId = 0;
					                		int32 lCountyCode = 0;
					                		if(llOldAcctId != imsNtfGroupHis.get_acctId())
					                		{// 减少数据库操作
					                			getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
					                			llOldCustId = llCustId;
					                			llOldAcctId = imsNtfGroupHis.get_acctId();
					                			lOldCountyCode = lCountyCode;
					                		}
					                		else
					                		{
					                			llCustId = llOldCustId;
					                			lCountyCode = lOldCountyCode;
					                		}
					                		imsNtfGroupHis.set_srcType("STS");
											imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
											imsNtfGroupHis.set_custId(llCustId);
											imsNtfGroupHis.set_assetAmount(imsNtfGrpstsErrHis.get_openAmount());
											imsNtfGroupHis.set_productId(llProductId);
											imsNtfGroupHis.set_updateTime(m_dtDateTime);
											imsNtfGroupHis.set_countyCode(lCountyCode);
											m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
										}
				                	}
				                }
				                // end add
                                grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                            }
                        }
                        else
                        {
                            MAbmInterfacePromptDb::CImsNtfGrpstserrHis imsNtfGrpstsErrHis;
                            transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsErrHis);
                            imsNtfGrpstsErrHis.set_status(ABM_PROMPT_PROCESS_FAILED);
                            //qiankun3 20190305 信控工单优化细分错单类型
                            if(CRM_RETURN_CODE_CRM_BOF_002 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfGrpstsErrHis.set_errStatus(AMB_CRM_CRM_BOF_002);
                            }
                            else if(CRM_RETURN_CODE_CRM_CUST_35 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfGrpstsErrHis.set_errStatus(ABM_CRM_CRM_CUST_35);
                            }
                            else if(CRM_RETURN_CODE_CRM_USER_112 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfGrpstsErrHis.set_errStatus(ABM_CRM_CRM_USER_112);
                            }
                            else if(CRM_RETURN_CODE_888888 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfGrpstsErrHis.set_errStatus(ABM_CRM_888888);
                            }
                            else if(CRM_RETURN_CODE_201711 == cErrorMsg.get_errorMsg())
                            {
                                imsNtfGrpstsErrHis.set_errStatus(ABM_CRM_201711);
                            }
                            imsNtfGrpstsErrHis.set_notifContent(strRemark.substr(0, 1024));
                            imsNtfGrpstsErrHis.set_soDate(m_dtDateTime);
                            m_listImsNtfGrpstsErrHis.push_back(imsNtfGrpstsErrHis);
                            // add by ligc@20200720 停开机工单入明细表判断
                            if (m_isNeedGroup)
                            {
                            	int64 llProductId = imsNtfGrpstsErrHis.get_productId();
				                if(judgeIsShow(llProductId, imsNtfGrpstsErrHis.get_regionCode()))
				                {
					               	MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
									tran2GroupHis(imsNtfGrpstsErrHis, imsNtfGroupHis);
									int32 iTradeTypeCode = 0;
									if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
									{
					               		int64 llCustId = 0;
					               		int32 lCountyCode = 0;
					               		if(llOldAcctId != imsNtfGroupHis.get_acctId())
					               		{// 减少数据库操作
					               			getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
					               			llOldCustId = llCustId;
					               			llOldAcctId = imsNtfGroupHis.get_acctId();
					               			lOldCountyCode = lCountyCode;
					               		}
					                		else
					               		{
					               			llCustId = llOldCustId;
					               			lCountyCode = lOldCountyCode;
					               		}
					               		imsNtfGroupHis.set_srcType("STS");
										imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
										imsNtfGroupHis.set_custId(llCustId);
										imsNtfGroupHis.set_assetAmount(imsNtfGrpstsErrHis.get_openAmount());
										imsNtfGroupHis.set_productId(llProductId);
										imsNtfGroupHis.set_updateTime(m_dtDateTime);
										imsNtfGroupHis.set_countyCode(lCountyCode);
										m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
									}
								}
							}
							// end add
                            grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                        }
                        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld ,acct_id = %lld come from sts sub_table, %s ：%s ,return ims_ntf_grpsts",
                            imsNtfGrpsts.get_queueId(),
                            imsNtfGrpsts.get_acctId(),
                            cErrorMsg.get_errorMsg().c_str(),
                            cErrorMsg.get_hint().c_str());
                        continue;
                    }

                    MAbmInterfacePromptDb::CImsNtfGrpstsHis imsNtfGrpstsHis;
                    transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);
                    imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                    if (llQueueId > 0)
                    {
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
                    }
                    else
                    {
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. ");
                    }
                    imsNtfGrpstsHis.set_soDate(m_dtDateTime);
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and removed to his.",
                        imsNtfGrpstsHis.get_queueId(),
                        llQueueId);
                    m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);
                    // add by ligc@20200720 停开机工单入明细表判断
                    if (m_isNeedGroup)
                    {
                    	int64 llProductId = imsNtfGrpstsHis.get_productId();
                    	if(judgeIsShow(llProductId, imsNtfGrpstsHis.get_regionCode()))
                    	{
                    		MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
                    		tran2GroupHis(imsNtfGrpstsHis, imsNtfGroupHis);
                    		int32 iTradeTypeCode = 0;
                    		if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
                    		{
                    			int64 llCustId = 0;
                    			int32 lCountyCode = 0;
                    			if(llOldAcctId != imsNtfGroupHis.get_acctId())
                    			{// 减少数据库操作
                    				getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
                    				llOldCustId = llCustId;
                    				llOldAcctId = imsNtfGroupHis.get_acctId();
                    				lOldCountyCode = lCountyCode;
                    			}
                    			else
                    			{
                    				llCustId = llOldCustId;
                    				lCountyCode = lOldCountyCode;
                    			}
                    			imsNtfGroupHis.set_srcType("STS");
                    			imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
                    			imsNtfGroupHis.set_custId(llCustId);
                    			imsNtfGroupHis.set_assetAmount(imsNtfGrpstsHis.get_openAmount());
                    			imsNtfGroupHis.set_productId(llProductId);
                    			imsNtfGroupHis.set_updateTime(m_dtDateTime);
                    			imsNtfGroupHis.set_countyCode(lCountyCode);
                    			m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
                    		}
                    	}
                    }
                    // end add
                    grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                }
            }
            //qiankun3 20190304 信控错单优化
            //将多次重做失败工单入错单表
            //将超时工单入错单表
            else if (ABMPROMPT_SLOWSPEED == retCode)//CRM侧返回需要降速，CRM错误码：CRM_TRADECREDIT_999
            {
                m_listImsNtfGrpstsErrHis.clear();
                m_listImsNtfGroupHis.clear();
                int64 llQueueId = 0;
                int64 llOldAcctId = 0;
                int64 llOldCustId = 0;
                int32 lOldCountyCode = 0;
                for (m_itrImsNtfGrpsts = m_listImsNtfGrpsts.begin(); m_itrImsNtfGrpsts != m_listImsNtfGrpsts.end(); ++m_itrImsNtfGrpsts)
                {
                    m_listImsNtfGrpstsErrHis.clear();
                    m_listImsNtfGroupHis.clear();
                    //m_dtDateTime = CBSDateTime::currentDateTime();
                    if (m_listImsNtfGrpsts.begin() == m_itrImsNtfGrpsts && m_itrImsNtfGrpsts->get_stateDtlId()<STS_START_STATE + m_iUpdateCrmExcep)
                    {
                        llQueueId = m_itrImsNtfGrpsts->get_queueId();
                        MAbmInterfacePromptDb::CImsNtfGrpsts imsNtfGrpsts = *m_itrImsNtfGrpsts;
                        imsNtfGrpsts.set_status(ABM_PROMPT_PROCESS_UNDO);//直接下次处理
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfGrpsts.set_notifContent(strRemark.substr(0, 1024));
                        imsNtfGrpsts.set_stateDtlId(imsNtfGrpsts.get_stateDtlId() + 1);
                        imsNtfGrpsts.set_soDate(m_dtDateTime.addSecs(60 * 5 * (imsNtfGrpsts.get_stateDtlId() - STS_START_STATE)));
                        LOG_TRACE(imsNtfGrpsts.to_string().c_str());
                        update_data<MAbmInterfacePromptDb::CImsNtfGrpstsList>(pSession, cErrorMsg, imsNtfGrpsts);
                        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld ,acct_id = %lld come from grp sts sub_table, %s ：%s ,return ims_ntf_grpsts",
                            imsNtfGrpsts.get_queueId(),
                            imsNtfGrpsts.get_acctId(),
                            cErrorMsg.get_errorMsg().c_str(),
                            cErrorMsg.get_hint().c_str());
                        continue;
                    }
                    MAbmInterfacePromptDb::CImsNtfGrpstserrHis imsNtfGrpstsErrHis;
					MAbmInterfacePromptDb::CImsNtfGrpstsHis imsNtfGrpstsHis;
                    
                    if (m_listImsNtfGrpsts.begin() == m_itrImsNtfGrpsts)
                    {
						transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsErrHis);
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        llQueueId = m_itrImsNtfGrpsts->get_queueId();
                        imsNtfGrpstsErrHis.set_notifContent(strRemark.substr(0,1024));
                        imsNtfGrpstsErrHis.set_status(ABM_PROMPT_PROCESS_PROC_TRY);
						imsNtfGrpstsErrHis.set_soDate(m_dtDateTime);
						m_listImsNtfGrpstsErrHis.push_back(imsNtfGrpstsErrHis);
                    }
                    else if (llQueueId > 0)
                    {
						transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);
                        imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
						imsNtfGrpstsHis.set_soDate(m_dtDateTime);
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. refer to queue_id: " + cdk::strings::Itoa(llQueueId));
						m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);
                        LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld   refer to queue_id=%lld is time out and remove to his.", imsNtfGrpstsHis.get_queueId(), llQueueId);
                    }

                    // add by ligc@20200720 停开机工单入明细表判断
                    if (m_isNeedGroup)
                    {
                    	int64 llProductId = m_itrImsNtfGrpsts->get_productId();
                    	if(judgeIsShow(llProductId, m_itrImsNtfGrpsts->get_regionCode()))
                    	{
                    		MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
                    		tran2GroupHis((*m_itrImsNtfGrpsts), imsNtfGroupHis);
                    		int32 iTradeTypeCode = 0;
                    		if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
                    		{
                    			int64 llCustId = 0;
                    			int32 lCountyCode = 0;
                    			if(llOldAcctId != imsNtfGroupHis.get_acctId())
                    			{// 减少数据库操作
                    				getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
                    				llOldCustId = llCustId;
                    				llOldAcctId = imsNtfGroupHis.get_acctId();
                    				lOldCountyCode = lCountyCode;
                    			}
                    			else
                    			{
                    				llCustId = llOldCustId;
                    				lCountyCode = lOldCountyCode;
                    			}
								imsNtfGroupHis.set_soDate(m_dtDateTime);
                    			imsNtfGroupHis.set_srcType("STS");
                    			imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
                    			imsNtfGroupHis.set_custId(llCustId);
                    			imsNtfGroupHis.set_assetAmount(m_itrImsNtfGrpsts->get_openAmount());
                    			imsNtfGroupHis.set_productId(llProductId);
                    			imsNtfGroupHis.set_updateTime(m_dtDateTime);
                    			imsNtfGroupHis.set_countyCode(lCountyCode);
                    			m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
                    		}
                    	}
                    }
                    // end add
                    
                    grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                }
            }
            //qiankun3 20190304 信控错单优化
            //将其他错误工单入错单表
            else// 其他错误
            {
                int64 llQueueId = 0;
                int64 llOldAcctId = 0;
                int64 llOldCustId = 0;
                int32 lOldCountyCode = 0;
                for (m_itrImsNtfGrpsts = m_listImsNtfGrpsts.begin(); m_itrImsNtfGrpsts != m_listImsNtfGrpsts.end(); ++m_itrImsNtfGrpsts)
                {
                    //生成历史表工单
                    m_listImsNtfGrpstsErrHis.clear();
                    m_listImsNtfGroupHis.clear();
                    if (m_itrImsNtfGrpsts == m_listImsNtfGrpsts.begin())
                    {
                        llQueueId = m_itrImsNtfGrpsts->get_queueId();
                        MAbmInterfacePromptDb::CImsNtfGrpsts imsNtfGrpsts = *m_itrImsNtfGrpsts;
                        imsNtfGrpsts.set_status(ABM_PROMPT_PROCESS_RETRY);  //2也表示错单，但是要重试的
                        AISTD string strRemark(cErrorMsg.get_errorMsg());
                        strRemark.append(":");
                        strRemark.append(cErrorMsg.get_hint());
                        strRemark.append(strCallCrmTime);
                        imsNtfGrpsts.set_notifContent(strRemark.substr(0, 1024));
                        imsNtfGrpsts.set_soDate(m_dtDateTime);
                        update_data<MAbmInterfacePromptDb::CImsNtfGrpstsList>(pSession, cErrorMsg, imsNtfGrpsts);
                        //统一提交
                        //contrl_commit(pSession,cErrorMsg);
                        LOG_ERROR(0, "\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld ,acct_id = %lld call crm is failed ,come from grp sts sub_table, %s ：%s ,return ims_ntf_grpsts",
                            imsNtfGrpsts.get_queueId(),
                            imsNtfGrpsts.get_acctId(),
                            cErrorMsg.get_errorMsg().c_str(),
                            cErrorMsg.get_hint().c_str());
                        continue;
                    }
                    MAbmInterfacePromptDb::CImsNtfGrpstsHis imsNtfGrpstsHis;
                    transToHis((*m_itrImsNtfGrpsts), imsNtfGrpstsHis);
                    imsNtfGrpstsHis.set_status(ABM_PROMPT_PROCESS_OUTTIME);
                    if (llQueueId > 0)
                    {
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. referred queue_id: " + cdk::strings::Itoa(llQueueId));
                    }
                    else
                    {
                        imsNtfGrpstsHis.set_notifContent("Out time orders, remove to his. ");
                    }
                    LOG_TRACE("\n==[ABM_SERV_ABMPROMPT]==> queue_id = %lld  is out time orders, refer to queue_id=%lld remove to his.", imsNtfGrpstsHis.get_queueId(), llQueueId);

                    imsNtfGrpstsHis.set_soDate(m_dtDateTime);
                    m_listImsNtfGrpstsHis.push_back(imsNtfGrpstsHis);
                    // add by ligc@20200720 停开机工单入明细表判断
                    if (m_isNeedGroup)
                    {
                    	int64 llProductId = imsNtfGrpstsHis.get_productId();
                    	if(judgeIsShow(llProductId, imsNtfGrpstsHis.get_regionCode()))
                    	{
                    		MAbmInterfacePromptDb::CImsNtfGroupHis imsNtfGroupHis;
                    		tran2GroupHis(imsNtfGrpstsHis, imsNtfGroupHis);
                    		int32 iTradeTypeCode = 0;
                    		if(ABMPROMPT_OK == getTradeTypeCode(imsNtfGroupHis.get_actionId(), iTradeTypeCode))
                    		{
                    			int64 llCustId = 0;
                    			int32 lCountyCode = 0;
                    			if(llOldAcctId != imsNtfGroupHis.get_acctId())
                    			{// 减少数据库操作
                    				getCustIdByAcctId(imsNtfGroupHis.get_acctId(), imsNtfGroupHis.get_regionCode(), llCustId, lCountyCode);
                    				llOldCustId = llCustId;
                    				llOldAcctId = imsNtfGroupHis.get_acctId();
                    				lOldCountyCode = lCountyCode;
                    			}
                    			else
                    			{
                    				llCustId = llOldCustId;
                    				lCountyCode = lOldCountyCode;
                    			}
                    			imsNtfGroupHis.set_srcType("STS");
                    			imsNtfGroupHis.set_tradeTypeCode(iTradeTypeCode);
                    			imsNtfGroupHis.set_custId(llCustId);
                    			imsNtfGroupHis.set_assetAmount(imsNtfGrpstsHis.get_openAmount());
                    			imsNtfGroupHis.set_productId(llProductId);
                    			imsNtfGroupHis.set_updateTime(m_dtDateTime);
                    			imsNtfGroupHis.set_countyCode(lCountyCode);
                    			m_listImsNtfGroupHis.push_back(imsNtfGroupHis);
                    		}
                    	}
                    }
                    // end add
                    grpstsInsertDelete(pSession, cErrorMsg, *m_itrImsNtfGrpsts);
                }
            }
            ES_END_RUN_TIME
        }

        if (iCount>=m_pCfg->m_cfgCommon.m_iCommitNum)
        {
            upStatPrompt(pSession, cErrorMsg);
            contrl_commit(pSession, cErrorMsg);
            iCount = 0;
        }


        if (1==retCode&&m_pCfg->m_cfgCommon.m_nSleep>0)
        {
            //由于速度太快，CRM处理不过来，需要等待几秒
            sleep(m_pCfg->m_cfgCommon.m_nSleep);
        }
        ES_END_RUN_TIME

        ABM_PROMPT_STAT
    }
    upStatPrompt(pSession, cErrorMsg);
    contrl_commit(pSession, cErrorMsg);

    LEAVE_FUNC
    return ABMPROMPT_OK;
}

//end mod

//飞书必达需求 notifyContent后拼接了msgType字段到,分割符为"|||",此函数将两者拆开
int32 ABMPrompt::split_msgType_notifyContent(
    AISTD string in_str,
    AISTD string in_divideStr,
    CStringList &out_list)
{

    AISTD string::size_type iPos = 0;

    if (!out_list.empty())
        out_list.clear();

    if (in_str.empty())
        return 0;

    if (in_divideStr.empty())
    {
        out_list.push_back(in_str);
        return 1;
    }

    iPos = in_str.find(in_divideStr);
    out_list.push_back(in_str.substr(0, iPos));
    LOG_TRACE("in_str.substr(0,iPos)：%s", in_str.substr(0, iPos).c_str());

    out_list.push_back(in_str.substr(iPos + in_divideStr.size(), in_str.length() - 1));
    LOG_TRACE("in_str.substr(iPos+in_divideStr.size(),in_str.length()-1)：%s", in_str.substr(iPos + in_divideStr.size(), in_str.length() - 1).c_str());

    return out_list.size();
}
