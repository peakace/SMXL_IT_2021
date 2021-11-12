// ----------------------------------------------
// SETTINGS


// Google Analytics Settings
const GOOGLE_ANALYTICS_VIEW_IDS = [
	'ga:123465798', // enter your google analytics view-id's here
	
];

const GOOGLE_CLOUD_PROJECT_ID = 'your_google_cloud_project_id';

const TRANSFER_SERVICE_DATASET = 'dataset_id_used_during_setup_of_transfer_service';

// In which location shall the datasets be created.
// Typicall choices: EU or US
// Location must be the same for all datasets.
const BIGQUERY_LOCATION = 'EU';

// Google Ads AccountId which is used in Transfer-Service
const GOOGLE_ADS_ACCOUNT_ID = 0123456789;


// Optional settings ----------------------------

const LIMIT_ANALYTICS_DATA_PER_EXECUTION_IN_DAYS = 1; // 1 day
const HOUR_INTERVAL = 1; // 1 = every hour, 2 = every 2 hours, ...


// These settings control how far into the past you wish to analyse data.
// Larger time spans cost more money ( Bigquery queries cost more the more partitions (days) they touch ).
// Ideally both settings should be set to the same value.
const MAX_DAYS_OF_ANALYTICS_DATA  = 365;
const MAX_DAYS_OF_GOOGLE_ADS_DATA = 365;


// Add this prefix to all pitfall views.
// This can be helpfull if the pitfall views share 
// a dataset with other views/tables because everything
// is ordered lexically and a common prefix will 
// group the pitfalls into a single range.
const PITFALL_PREFIX = '';

const CACHE_EVERY_HOURS = 10;

// ------ END of Settings ----------------------
// ---------------------------------------------


// Transfer-Service dataset --------------------
const TS_DATASET  = {
	projectId : GOOGLE_CLOUD_PROJECT_ID,
	datasetId : TRANSFER_SERVICE_DATASET,
};

const STAY_IN_CONTROL_DATASET = {
	projectId : GOOGLE_CLOUD_PROJECT_ID,
	datasetId : 'smxl_it_2021_stay_in_control',
};

// Pitfalls dataset ----------------------------
const PITFALLS_DATASET = {
	projectId : GOOGLE_CLOUD_PROJECT_ID,
	datasetId : 'smxl_it_2021_pitfalls',
};

// Cookie Consent Checker dataset --------------
const CONSENT_CHECKER_DATASET = {
	projectId : GOOGLE_CLOUD_PROJECT_ID,
	datasetId : 'cookie_consent_check',
};

const CONSENT_CHECKER_TABLE = {
	...CONSENT_CHECKER_DATASET,
	tableId   : 'cookie_consent_check',
};

const CONSENT_CHECKER_VIEW = {
	...CONSENT_CHECKER_DATASET,
	tableId   : 'cookie_consent_checker',
};

const CACHED_PREFIX = '1_';

// ---------------------------
// CONSTANTS
const MAX_RESULTS         = 10000; // analytics API never returns more than 10k results per page
const MAX_ALLOWED_METRICS = 10;    // analytics API allows only 10
// END OF CONSTANTS
// ---------------------------

const PITFALLS_DEPENDING_ON_AUGMENTER_SCRIPT = [
	'CAMPAIGN_MULTI_CHANNEL',
	'CAMPAIGN_TARGET_AND_BID',
	'CAMPAIGN_NON_STANDARD_DELIVERY_METHOD',
	'CAMPAIGN_ROTATION_TYPE_NOT_OPTIMIZED',
	'EXTENSION_NO_SITE_LINKS',
	'EXTENSION_TOO_FEW_SITE_LINKS',
	'EXTENSION_NO_CALLOUTS',
	'EXTENSION_TOO_FEW_CALLOUTS',
	'EXTENSION_NO_SNIPPETS',
	'EXTENSION_TOO_FEW_SNIPPETS',
	'EXTENSION_NO_MESSAGES',
	'EXTENSION_NO_PHONE_NUMBERS',
	'EXTENSION_NO_MOBILE_APPS',
	'MISSING_EXCLUDED_CONTENT_LABELS', // campaign_settings
	'DISAPPROVED_EXTENSIONS', // all extensions
	'DOMINATED_NEGATIVE_KEYWORDS_IN_CAMPAIGNS', // CAMPAIGN_NEGATIVE_KEYWORDS
	'NEGATIVE_KEYWORD_IN_AD', // CAMPAIGN_NEGATIVE_KEYWORDS
];

const ABSORBED_PITFALLS = [
	'extension_no_callouts',
	'extension_no_messages',
	'extension_no_mobile_apps',
	'extension_no_phone_numbers',
	'extension_no_site_links',
	'extension_no_snippets',
	'extension_too_few_callouts',
	'extension_too_few_site_links',
	'extension_too_few_snippets',

	'campaign_missing_mobile_bid_modifier',
	'campaign_multi_channel',
	'campaign_non_standard_delivery_method',
	'campaign_rotation_type_not_optimized',
	'campaign_target_and_bid',
	'campaign_non_brand_no_smart_bidding',

	'adgroup_too_many_keywords',
	'adgroup_no_active_keywords',
	'adgroup_no_ads',
	'adgroup_no_dsa',
	'adgroup_no_keywords',
	'adgroup_no_negative_keywords_in_broad_adgroup',
	'adgroup_too_few_ads',
	'adgroup_too_few_dsa',
	'adgroup_too_many_broad_keywords',
	'adgroup_too_many_dsa',
	'adgroup_too_many_enabled_ads',
	'adgroup_too_many_exact_keywords',
	'adgroup_without_audience',
	'adgroup_multiple_poor_relevance_keywords',
	'adgroup_no_rsa',
	'adgroup_too_many_rsa',

	'keyword_adgroup_match_type_mismatch',
	'keyword_campaign_match_type_mismatch',
	'keyword_contains_capital',
	'keyword_contains_dash',
	'keyword_contains_dot',
	'keyword_modified_negative',
	'keyword_contains_plus',
	'keyword_session_id_in_url',
	'keyword_target_url_missing',
	'keyword_target_url_multiple_question_marks',
	'keyword_broad_match_still_active',
	'keyword_bmm_still_active',
	'keyword_bad_qs',
	'keyword_below_average_ad_relevance',
	'keyword_below_average_expected_ctr',
	'keyword_below_average_lp_experiance',
	'keyword_contains_brackets',
	'keyword_contains_quotes',

	'ad_duplicate_special_chars_in_description',
	'ad_last_char_is_not_special',
	'ad_path1_missing_in_non_brand_campaign',
	'ad_path2_missing_in_non_brand_campaign',
	'ad_policy_violation',
	'ad_too_short_description',
	'ad_too_short_headline',
];

const CONSENT_CHECKER_QUERY = [ `WITH base AS (
	SELECT
		timestamp,
		REGEXP_EXTRACT( URI, "eventName=([^&]+)"       ) AS Event_Name,      
		REGEXP_EXTRACT( URI, "cookieValue=([^&]+)"     ) AS Cookie_Value,
		REGEXP_EXTRACT( URI, "consent=([^&]+)"         ) AS Click_Text,
		REGEXP_EXTRACT( URI, "consentStatus=([^&]+)"   ) AS Consent_Status,
		REGEXP_EXTRACT( URI, "url=([^&]+)"             ) AS Page_Path,
		REGEXP_EXTRACT( URI, "channel=([^&]+)"         ) AS Source_Medium,
		REGEXP_EXTRACT( URI, "bannerVersion=([^&]+)"   ) AS Banner_Version,
		REGEXP_EXTRACT( URI, "source_campaign=([^&]+)" ) AS Campaign,
		REGEXP_EXTRACT( URI, "source_content=([^&]+)"  ) AS Content,
		REGEXP_EXTRACT( URI, "source_term=([^&]+)"     ) AS Term,
		REGEXP_EXTRACT( URI, "agent=([^&]+)"           ) AS User_Agent,
		REGEXP_EXTRACT( URI, "browser=([^&]+)"         ) AS Browser,
		REGEXP_EXTRACT( URI, "os=([^&]+)"              ) AS Operating_System,
		REGEXP_EXTRACT( URI, "osVersion=([^&]+)"       ) AS Operating_System_Version,
		REGEXP_EXTRACT( URI, "language=([^&]+)"        ) AS Language,
		REGEXP_EXTRACT( URI, "isBot=([^&]+)"           ) AS is_Bot,
		REGEXP_EXTRACT( URI, "botName=([^&]+)"         ) AS Bot_Name,
		REGEXP_EXTRACT( URI, "deviceType=([^&]+)"      ) AS Device_Type,
		DATE( timestamp                                ) AS Date,
	FROM \`${ Object.values( CONSENT_CHECKER_TABLE ).join( '.' ) }\`
)
SELECT *,
	CASE WHEN Event_Name IN ("bannerimpression") THEN 1 ELSE 0 END as bannerImpression,
	CASE WHEN Event_Name IN ("bannerclick") THEN 1 ELSE 0 END as bannerClick,        
FROM base` ][ 0 ];

function main(){
	registerTrigger();
	
	createTablesAndViewsPitfall();
	createTablesAndViewsStayInControl();
	createTablesAndViewsConsentChecker();
	
	importGaData2();
}

function registerTrigger(){
	// check if we need to register a trigger
	if( ! ScriptApp.getProjectTriggers().map( trigger => trigger.getHandlerFunction() ).includes( main.name ) ){
		Logger.log( 'No triggers found for function main(). Register a new hourly trigger.' );
		ScriptApp
			.newTrigger( main.name )
			.timeBased()
			.everyHours( HOUR_INTERVAL )
			.create()
		;
	}
}

const BQ = {
	LOCATION                            : BIGQUERY_LOCATION,
	PROJECT_ID                          : STAY_IN_CONTROL_DATASET.projectId,
	STAY_IN_CONTROL_DATASET             : STAY_IN_CONTROL_DATASET,
	CONSENT_CHECKER_DATASET             : CONSENT_CHECKER_DATASET,
	CONSENT_CHECKER_TABLE               : CONSENT_CHECKER_TABLE,
	CONSENT_CHECKER_VIEW                : CONSENT_CHECKER_VIEW,
	
	ANALYTICS_DATA                      : { ...STAY_IN_CONTROL_DATASET, tableId : 'analytics_data'                    },
	GHOST_TERMS                         : { ...STAY_IN_CONTROL_DATASET, tableId : 'ghost_terms'                       },
	GHOST_TERMS_BY_AD_NETWORK           : { ...STAY_IN_CONTROL_DATASET, tableId : 'ghost_terms_by_ad_network'         },
	GHOST_NGRAMS                        : { ...STAY_IN_CONTROL_DATASET, tableId : 'ghost_ngrams'                      },
	MISSING                             : { ...STAY_IN_CONTROL_DATASET, tableId : 'missing'                           },
	MISSROUTING_QUERIES                 : { ...STAY_IN_CONTROL_DATASET, tableId : 'missrouting_queries'               },
	MISSING_DSA_KEYWORDS                : { ...STAY_IN_CONTROL_DATASET, tableId : 'missing_dsa_keywords'              },
	MISSING_KEYWORDS                    : { ...STAY_IN_CONTROL_DATASET, tableId : 'missing_keywords'                  },
	PAUSED_KEYWORDS                     : { ...STAY_IN_CONTROL_DATASET, tableId : 'paused_keywords'                   },
	ROUTING_PROBLEMS                    : { ...STAY_IN_CONTROL_DATASET, tableId : 'routing_problems'                  },
	SHOPPING_MISSING_KEYWORDS           : { ...STAY_IN_CONTROL_DATASET, tableId : 'shopping_missing_keywords'         },
	COUNTS                              : { ...STAY_IN_CONTROL_DATASET, tableId : 'counts'                            },
	NGRAM                               : { ...STAY_IN_CONTROL_DATASET, tableId : 'ngram'                             },
	RAW_DATA                            : { ...STAY_IN_CONTROL_DATASET, tableId : 'raw_data'                          },
	
	AD                                  : { ...TS_DATASET,              tableId : 'Ad_'                                + GOOGLE_ADS_ACCOUNT_ID },
	CUSTOMER                            : { ...TS_DATASET,              tableId : 'Customer_'                          + GOOGLE_ADS_ACCOUNT_ID },
	CAMPAIGN                            : { ...TS_DATASET,              tableId : 'Campaign_'                          + GOOGLE_ADS_ACCOUNT_ID },
	AD_GROUP                            : { ...TS_DATASET,              tableId : 'AdGroup_'                           + GOOGLE_ADS_ACCOUNT_ID },
	KEYWORD                             : { ...TS_DATASET,              tableId : 'Keyword_'                           + GOOGLE_ADS_ACCOUNT_ID },
	CRITERIA                            : { ...TS_DATASET,              tableId : 'Criteria_'                          + GOOGLE_ADS_ACCOUNT_ID },
	LOCATION_BASED_CAMPAIGN_CRITERION   : { ...TS_DATASET,              tableId : 'LocationBasedCampaignCriterion_'    + GOOGLE_ADS_ACCOUNT_ID },
	SEARCH_QUERY_STATS                  : { ...TS_DATASET,              tableId : 'SearchQueryStats_'                  + GOOGLE_ADS_ACCOUNT_ID },
	KEYWORD_BASIC_STATS                 : { ...TS_DATASET,              tableId : 'KeywordBasicStats_'                 + GOOGLE_ADS_ACCOUNT_ID },
	AD_GROUP_STATS                      : { ...TS_DATASET,              tableId : 'AdGroupStats_'                      + GOOGLE_ADS_ACCOUNT_ID },
	KEYWORD_CROSS_DEVICE_STATS          : { ...TS_DATASET,              tableId : 'KeywordCrossDeviceStats_'           + GOOGLE_ADS_ACCOUNT_ID },
	CAMPAIGN_BASIC_STATS                : { ...TS_DATASET,              tableId : 'CampaignBasicStats_'                + GOOGLE_ADS_ACCOUNT_ID },
	CAMPAIGN_CROSS_DEVICE_STATS         : { ...TS_DATASET,              tableId : 'CampaignCrossDeviceStats_'          + GOOGLE_ADS_ACCOUNT_ID },
	CRITERIA_CONVERSION_STATS           : { ...TS_DATASET,              tableId : 'CriteriaConversionStats_'           + GOOGLE_ADS_ACCOUNT_ID },

	CAMPAIGN_SETTINGS                   : { ...TS_DATASET,              tableId : 'CampaignSettings_'                  + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_CAMPAIGN_MAP             : { ...TS_DATASET,              tableId : 'ExtensionsCampaignMap_'             + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_DISAPPROVED              : { ...TS_DATASET,              tableId : 'ExtensionsDisapproved_'             + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_SITELINKS                : { ...TS_DATASET,              tableId : 'ExtensionsSitelinks_'               + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_CALLOUTS                 : { ...TS_DATASET,              tableId : 'ExtensionsCallouts_'                + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_MESSAGES                 : { ...TS_DATASET,              tableId : 'ExtensionsMessages_'                + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_MOBILE_APPS              : { ...TS_DATASET,              tableId : 'ExtensionsMobileApps_'              + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_PHONE_NUMBERS            : { ...TS_DATASET,              tableId : 'ExtensionsPhoneNumbers_'            + GOOGLE_ADS_ACCOUNT_ID },
	EXTENSIONS_SNIPPETS                 : { ...TS_DATASET,              tableId : 'ExtensionsSnippets_'                + GOOGLE_ADS_ACCOUNT_ID },
	CAMPAIGN_NEGATIVE_KEYWORDS          : { ...TS_DATASET,              tableId : 'CampaignNegativeKeywords_'          + GOOGLE_ADS_ACCOUNT_ID },

	PITFALL_AD                          : { ...PITFALLS_DATASET,        tableId : PITFALL_PREFIX + 'ad'         },
	PITFALL_KEYWORD                     : { ...PITFALLS_DATASET,        tableId : PITFALL_PREFIX + 'keyword'    },
	PITFALL_AD_GROUP                    : { ...PITFALLS_DATASET,        tableId : PITFALL_PREFIX + 'ad_group'   },
	PITFALL_CAMPAIGN                    : { ...PITFALLS_DATASET,        tableId : PITFALL_PREFIX + 'campaign'   },
	PITFALL_EXTENSION                   : { ...PITFALLS_DATASET,        tableId : PITFALL_PREFIX + 'extension'  },

};

function pitfallReference( pitfall ){
	return '`' + Object.values( { ...PITFALLS_DATASET, tableId : PITFALL_PREFIX + pitfall  } ).join( '.' ) + '`';
}

function tsReference( viewBaseName ){
	return '`' + Object.values( { ...TS_DATASET, tableId : viewBaseName + '_' + GOOGLE_ADS_ACCOUNT_ID } ).join( '.' ) + '`';
}

const STAY_IN_CONTROL_VIEWS = [
	{ // ghost_terms
		reference : BQ.GHOST_TERMS,
		query     : `WITH
ga AS (
	SELECT
		CAST( adwordsCustomerID AS INT64 )             AS ExternalCustomerId,
		CAST( adwordsCampaignID AS INT64 )             AS CampaignId,
		CAST( adwordsAdgroupID	AS INT64 )             AS AdGroupId,
		CAST( adwordsCriteriaId AS INT64 )             AS CriterionId,
		STRING_AGG( DISTINCT adMatchType, ', ' )       AS GaMatchType,
		adMatchedQuery                                 AS Query,
		SUM( sessions )                                AS GaSessions,
		SUM( goal1Completions            )             AS goal1Completions,
		SUM( goal2Completions            )             AS goal2Completions,
		SUM( goal3Completions            )             AS goal3Completions,
		SUM( goal4Completions            )             AS goal4Completions,
		SUM( goal5Completions            )             AS goal5Completions,
		SUM( goal6Completions            )             AS goal6Completions,
		SUM( goal7Completions            )             AS goal7Completions,
		SUM( goal8Completions            )             AS goal8Completions,
		SUM( goal9Completions            )             AS goal9Completions,
		SUM( goal10Completions           )             AS goal10Completions,
		SUM( goal11Completions           )             AS goal11Completions,
		SUM( goal12Completions           )             AS goal12Completions,
		SUM( goal13Completions           )             AS goal13Completions,
		SUM( goal14Completions           )             AS goal14Completions,
		SUM( goal15Completions           )             AS goal15Completions,
		SUM( goal16Completions           )             AS goal16Completions,
		SUM( goal17Completions           )             AS goal17Completions,
		SUM( goal18Completions           )             AS goal18Completions,
		SUM( goal19Completions           )             AS goal19Completions,
		SUM( goal20Completions           )             AS goal20Completions,
		SUM( transactions                )             AS transactions,
		SUM( transactionRevenue          )             AS transactionRevenue,
		Date,
	FROM \`${ Object.values( BQ.ANALYTICS_DATA ).join( '.' ) }\`
	WHERE TRUE
		AND adMatchedQuery    != '(not set)'
		AND adwordsCustomerID != '(not set)'
		AND adwordsCampaignID != '(not set)'
		AND adwordsAdgroupID  != '(not set)'
		AND adwordsCriteriaId != '(not set)'
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Query,
		Date
),
gaStats AS (
	SELECT
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria,
		ga.*,
	FROM ga
	LEFT JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` customer
		ON TRUE
		AND ga.ExternalCustomerId = customer.ExternalCustomerId
		AND customer._DATA_DATE   = customer._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` campaign
		ON TRUE
		AND ga.ExternalCustomerId = campaign.ExternalCustomerId
		AND ga.CampaignId         = campaign.CampaignId
		AND campaign._DATA_DATE   = campaign._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` adGroup
		ON TRUE
		AND ga.ExternalCustomerId = adGroup.ExternalCustomerId
		AND ga.CampaignId         = adGroup.CampaignId
		AND ga.AdGroupId          = adGroup.AdGroupId
		AND adGroup._DATA_DATE    = adGroup._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` keyword
		ON TRUE
		AND ga.ExternalCustomerId = keyword.ExternalCustomerId
		AND ga.CampaignId         = keyword.CampaignId
		AND ga.AdGroupId          = keyword.AdGroupId
		AND ga.CriterionId        = keyword.CriterionId
		AND keyword._DATA_DATE    = keyword._LATEST_DATE
	WHERE TRUE
),
sqStats AS (
	SELECT
		ExternalCustomerId,
		Query,
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	WHERE TRUE
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		Query
),
keywordStats AS (
	SELECT
		stats.ExternalCustomerId,
		stats.CampaignId,
		stats.AdGroupId,
		stats.CriterionId,
		ANY_VALUE( CustomerDescriptiveName )     AS CustomerDescriptiveName,
		ANY_VALUE( CampaignName            )     AS CampaignName,
		ANY_VALUE( AdGroupName             )     AS AdGroupName,
		ANY_VALUE( Criteria                )     AS Criteria,
		Date,
		ANY_VALUE( KeywordMatchType )            AS KeywordMatchType,
		       SUM( Impressions )                AS Impressions,
		ROUND( SUM( Cost        ) / 1000000, 2 ) AS Cost,
		       SUM( Clicks      )                AS Clicks,
		ROUND( SUM( Conversions ),           2 ) AS Conversions,
	FROM \`${ Object.values( BQ.KEYWORD_BASIC_STATS ).join( '.' ) }\` stats
	LEFT JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` customer
		ON TRUE
		AND stats.ExternalCustomerId = customer.ExternalCustomerId
		AND customer._DATA_DATE      = customer._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` campaign
		ON TRUE
		AND stats.ExternalCustomerId = campaign.ExternalCustomerId
		AND stats.CampaignId         = campaign.CampaignId
		AND campaign._DATA_DATE      = campaign._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` adGroup
		ON TRUE
		AND stats.ExternalCustomerId = adGroup.ExternalCustomerId
		AND stats.CampaignId         = adGroup.CampaignId
		AND stats.AdGroupId          = adGroup.AdGroupId
		AND adGroup._DATA_DATE       = adGroup._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` keyword
		ON TRUE
		AND stats.ExternalCustomerId = keyword.ExternalCustomerId
		AND stats.CampaignId         = keyword.CampaignId
		AND stats.AdGroupId          = keyword.AdGroupId
		AND stats.CriterionId        = keyword.CriterionId
		AND keyword._DATA_DATE       = keyword._LATEST_DATE
	WHERE TRUE
		AND stats.AdNetworkType2 IN ( 'SEARCH', 'SEARCH_PARTNERS' )
		AND DATE_DIFF( stats._LATEST_DATE, stats._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Date
),
ghostQueries AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		gaStats.CustomerDescriptiveName,
		gaStats.CampaignName,
		gaStats.AdGroupName,
		gaStats.Criteria,
		Query,
		Date,
		GaSessions,
		ROUND( keywordStats.Cost / NULLIF( keywordStats.Clicks, 0 ), 2 ) AS CPC,
		ROUND( keywordStats.Cost / NULLIF( keywordStats.Clicks, 0 ) * GaSessions, 2 ) AS EstimatedCost,
		goal1Completions,
		goal2Completions,
		goal3Completions,
		goal4Completions,
		goal5Completions,
		goal6Completions,
		goal7Completions,
		goal8Completions,
		goal9Completions,
		goal10Completions,
		goal11Completions,
		goal12Completions,
		goal13Completions,
		goal14Completions,
		goal15Completions,
		goal16Completions,
		goal17Completions,
		goal18Completions,
		goal19Completions,
		goal20Completions,
		transactions,
		transactionRevenue,
	FROM gaStats
	LEFT JOIN sqStats      USING ( ExternalCustomerId, Query )
	LEFT JOIN keywordStats USING ( ExternalCustomerId, CampaignId, AdGroupId, CriterionId, Date )
	WHERE TRUE
		AND sqStats.ExternalCustomerId IS NULL
)
SELECT
	ExternalCustomerId,
	CampaignId,
	AdGroupId,
	CriterionId,
	CustomerDescriptiveName,
	CampaignName,
	AdGroupName,
	Criteria,
	Query,
	Date,
	GaSessions,
	CPC,
	EstimatedCost,
	goal1Completions,
	goal2Completions,
	goal3Completions,
	goal4Completions,
	goal5Completions,
	goal6Completions,
	goal7Completions,
	goal8Completions,
	goal9Completions,
	goal10Completions,
	goal11Completions,
	goal12Completions,
	goal13Completions,
	goal14Completions,
	goal15Completions,
	goal16Completions,
	goal17Completions,
	goal18Completions,
	goal19Completions,
	goal20Completions,
	transactions,
	transactionRevenue,
FROM ghostQueries
WHERE TRUE
ORDER BY
	GaSessions DESC
`,
	},
	{ // ghost_terms_by_ad_network
		reference : BQ.GHOST_TERMS_BY_AD_NETWORK,
		query     : `WITH
sqStats AS (
	SELECT
		IFNULL( AdvertisingChannelType,    '--' ) AS AdvertisingChannelType,
		IFNULL( AdvertisingChannelSubType, '--' ) AS AdvertisingChannelSubType,
		IFNULL( AdNetworkType2,            '--' ) AS AdNetworkType2,
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria,
		'SQ' AS Type,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		ROUND( SUM( Conversions ), 2 ) AS Conversions,
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` stat
	JOIN \`${ Object.values( BQ.CUSTOMER           ).join( '.' ) }\` customer USING( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN           ).join( '.' ) }\` campaign USING( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP           ).join( '.' ) }\` adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
	JOIN \`${ Object.values( BQ.KEYWORD            ).join( '.' ) }\` keyword  USING( ExternalCustomerId, CampaignId, AdGroupId, CriterionId )
	WHERE TRUE
		AND DATE_DIFF( CURRENT_DATE(), stat._DATA_DATE, DAY ) > 0
		AND DATE_DIFF( CURRENT_DATE(), stat._DATA_DATE, DAY ) <= 90
		AND customer._DATA_DATE = customer._LATEST_DATE
		AND campaign._DATA_DATE = campaign._LATEST_DATE
		AND adgroup._DATA_DATE = adgroup._LATEST_DATE
		AND keyword._DATA_DATE = keyword._LATEST_DATE
	GROUP BY
		AdNetworkType2,
		AdvertisingChannelType,
		AdvertisingChannelSubType,
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria
),
kwStats AS (
	SELECT
		IFNULL( AdvertisingChannelType,    '--' ) AS AdvertisingChannelType,
		IFNULL( AdvertisingChannelSubType, '--' ) AS AdvertisingChannelSubType,
		IFNULL( AdNetworkType2,            '--' ) AS AdNetworkType2,
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria,
		'Keyword' AS Type,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		ROUND( SUM( Conversions ), 2 ) AS Conversions,
	FROM \`${ Object.values( BQ.KEYWORD_BASIC_STATS ).join( '.' ) }\` stat
	JOIN \`${ Object.values( BQ.CUSTOMER            ).join( '.' ) }\` customer USING( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN            ).join( '.' ) }\` campaign USING( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP            ).join( '.' ) }\` adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
	JOIN \`${ Object.values( BQ.KEYWORD             ).join( '.' ) }\` keyword  USING( ExternalCustomerId, CampaignId, AdGroupId, CriterionId )
	WHERE TRUE
		AND DATE_DIFF( CURRENT_DATE(), stat._DATA_DATE, DAY ) > 0
		AND DATE_DIFF( CURRENT_DATE(), stat._DATA_DATE, DAY ) <= 90
		AND customer._DATA_DATE = customer._LATEST_DATE
		AND campaign._DATA_DATE = campaign._LATEST_DATE
		AND adgroup._DATA_DATE = adgroup._LATEST_DATE
		AND keyword._DATA_DATE = keyword._LATEST_DATE
		AND Criteria NOT IN ( 'AutomaticContent', 'AutomaticKeywords' )
	GROUP BY
		AdNetworkType2,
		AdvertisingChannelType,
		AdvertisingChannelSubType,
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria
),
result AS (
	SELECT
		AdvertisingChannelType,
		AdvertisingChannelSubType,
		AdNetworkType2      AS AdNetwork,
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria,
		kwStats.Impressions AS kwImpressions,
		kwStats.Clicks      AS kwClicks,
		kwStats.Conversions AS kwConversions,
		
		sqStats.Impressions AS sqImpressions,
		sqStats.Clicks      AS sqClicks,
		sqStats.Conversions AS sqConversions,
		
		kwStats.Impressions         - sqStats.Impressions      AS missingImpressions,
		kwStats.Clicks              - sqStats.Clicks           AS missingClicks,
		ROUND( kwStats.Conversions  - sqStats.Conversions, 2 ) AS missingConversions,
		
		--ROUND( sqStats.Impressions / NULLIF( kwStats.Impressions, 0 ) , 2 ) AS ImpressionRatio,
		--ROUND( sqStats.Clicks      / NULLIF( kwStats.Clicks     , 0 ) , 2 ) AS ClicksRatio,
		--ROUND( sqStats.Conversions / NULLIF( kwStats.Conversions, 0 ) , 2 ) AS ConversionsRatio,
	FROM kwStats
	FULL OUTER JOIN sqStats USING (
		AdNetworkType2,
		AdvertisingChannelType,
		AdvertisingChannelSubType,
		CustomerDescriptiveName,
		CampaignName,
		AdGroupName,
		Criteria
	)
	ORDER BY kwImpressions DESC
)
SELECT
	*,
FROM result`,
	},
	{ // missing
		reference : BQ.MISSING,
		query     : `WITH
sqStats AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Query,
		Date,
		SUM( Impressions ) AS Impressions,
		SUM( Cost        ) / 1000000 AS Cost,
		SUM( Clicks      ) AS Clicks,
		SUM( Conversions ) AS Conversions,
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` stats
	WHERE TRUE
		AND DATE_DIFF( _LATEST_DATE, _DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Query,
		Date
),
keywordStats AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		ANY_VALUE( CustomerDescriptiveName )     AS CustomerDescriptiveName,
		ANY_VALUE( CampaignName )                AS CampaignName,
		ANY_VALUE( AdGroupName  )                AS AdGroupName,
		ANY_VALUE( Criteria     )                AS Criteria,
		Date,
		ANY_VALUE( KeywordMatchType )            AS KeywordMatchType,
		       SUM( Impressions )                AS Impressions,
		ROUND( SUM( Cost        ) / 1000000, 2 ) AS Cost,
		       SUM( Clicks      )                AS Clicks,
		ROUND( SUM( Conversions ), 2 )           AS Conversions,
	FROM \`${ Object.values( BQ.KEYWORD_BASIC_STATS ).join( '.' ) }\` stats
	LEFT JOIN \`${ Object.values( BQ.CUSTOMER       ).join( '.' ) }\` customer
		USING ( ExternalCustomerId )
	LEFT JOIN \`${ Object.values( BQ.CAMPAIGN       ).join( '.' ) }\` campaign
		USING ( ExternalCustomerId, CampaignId )
	LEFT JOIN \`${ Object.values( BQ.AD_GROUP       ).join( '.' ) }\` adGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	LEFT JOIN \`${ Object.values( BQ.KEYWORD        ).join( '.' ) }\` keyword
		USING ( ExternalCustomerId, CampaignId, AdGroupId, CriterionId )
	WHERE TRUE
		AND stats.AdNetworkType2 IN ( 'SEARCH', 'SEARCH_PARTNERS' )
		AND customer._DATA_DATE = customer._LATEST_DATE
		AND campaign._DATA_DATE = campaign._LATEST_DATE
		AND adGroup._DATA_DATE  = adGroup._LATEST_DATE
		AND keyword._DATA_DATE  = keyword._LATEST_DATE
		AND DATE_DIFF( stats._LATEST_DATE, stats._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Date
),
missing AS (
	SELECT
		ANY_VALUE( CustomerDescriptiveName ) AS CustomerDescriptiveName,
		ANY_VALUE( CampaignName )            AS CampaignName,
		ANY_VALUE( AdGroupName  )            AS AdGroupName,
		ANY_VALUE( Criteria     )            AS Criteria,
		Date,
		ANY_VALUE( KeywordMatchType )                                         AS KeywordMatchType,
		ANY_VALUE( keywordStats.Impressions )                                 AS keywordImpressions,
		ANY_VALUE( keywordStats.Cost )                                        AS keywordCost,
		ANY_VALUE( keywordStats.Clicks )                                      AS keywordClicks,
		ROUND( ANY_VALUE( keywordStats.Conversions ), 2 )                     AS keywordConversions,
		
		IFNULL( SUM( sqStats.Impressions ), 0 )                               AS sqImpressions,
		IFNULL( ROUND( SUM( sqStats.Cost ), 2 ), 0 )                          AS sqCost,
		IFNULL( SUM( sqStats.Clicks      ), 0 )                               AS sqClicks,
		IFNULL( ROUND( SUM( sqStats.Conversions ), 2 ), 0 )                   AS sqConversions,
		       ANY_VALUE( keywordStats.Impressions ) - IFNULL( SUM( sqStats.Impressions ), 0 )      AS MissingImpressions,
		ROUND( ANY_VALUE( keywordStats.Cost )        - IFNULL( SUM( sqStats.Cost        ), 0 ), 2 ) AS MissingCost,
		       ANY_VALUE( keywordStats.Clicks )      - IFNULL( SUM( sqStats.Clicks      ), 0 )      AS MissingClicks,
		ROUND( ANY_VALUE( keywordStats.Conversions ) - IFNULL( SUM( sqStats.Conversions ), 0 ), 2 ) AS MissingConversions,
		STRING_AGG( DISTINCT Query ) AS Queries,
	FROM keywordStats
	LEFT JOIN sqStats USING (
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Date
	)
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Date
),
performance AS (
  SELECT
    ROUND( SUM( KeywordConversions ) / IFNULL( SUM( KeywordClicks ), 0 ) * 100, 2 ) AS KeywordCr,
    ROUND( SUM( SqConversions      ) / IFNULL( SUM( SqClicks      ), 0 ) * 100, 2 ) AS SqCr,
    ROUND( SUM( MissingConversions ) / IFNULL( SUM( MissingClicks ), 0 ) * 100, 2 ) AS MissingCr,
  FROM missing
)
SELECT
  *
FROM missing
WHERE TRUE
--ORDER BY MissingImpressions DESC`,
	},
	{ // ghost_ngrams
		reference : BQ.GHOST_NGRAMS,
		query :
			`WITH
ghostQueries AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		ANY_VALUE( CustomerDescriptiveName ) AS CustomerDescriptiveName,
		ANY_VALUE( CampaignName            ) AS CampaignName,
		ANY_VALUE( AdGroupName             ) AS AdGroupName,
		ANY_VALUE( Criteria                ) AS Criteria,
		Query,
		Date,
		SUM( EstimatedCost               )             AS EstimatedCost,
		SUM( GaSessions                  )             AS GaSessions,
		SUM( goal1Completions            )             AS goal1Completions,
		SUM( goal2Completions            )             AS goal2Completions,
		SUM( goal3Completions            )             AS goal3Completions,
		SUM( goal4Completions            )             AS goal4Completions,
		SUM( goal5Completions            )             AS goal5Completions,
		SUM( goal6Completions            )             AS goal6Completions,
		SUM( goal7Completions            )             AS goal7Completions,
		SUM( goal8Completions            )             AS goal8Completions,
		SUM( goal9Completions            )             AS goal9Completions,
		SUM( goal10Completions           )             AS goal10Completions,
		SUM( goal11Completions           )             AS goal11Completions,
		SUM( goal12Completions           )             AS goal12Completions,
		SUM( goal13Completions           )             AS goal13Completions,
		SUM( goal14Completions           )             AS goal14Completions,
		SUM( goal15Completions           )             AS goal15Completions,
		SUM( goal16Completions           )             AS goal16Completions,
		SUM( goal17Completions           )             AS goal17Completions,
		SUM( goal18Completions           )             AS goal18Completions,
		SUM( goal19Completions           )             AS goal19Completions,
		SUM( goal20Completions           )             AS goal20Completions,
		SUM( transactions                )             AS transactions,
		SUM( transactionRevenue          )             AS transactionRevenue,
	FROM \`${ Object.values( BQ.GHOST_TERMS ).join( '.' ) }\`
	GROUP BY
		ExternalCustomerId,
    CampaignId,
    AdGroupId,
    CriterionId,
		Query,
		Date
),
ngrams AS (
	SELECT
		ExternalCustomerId,
    CampaignId,
    AdGroupId,
    CriterionId,
    ANY_VALUE( CustomerDescriptiveName ) AS CustomerDescriptiveName,
    ANY_VALUE( CampaignName            ) AS CampaignName,
    ANY_VALUE( AdGroupName             ) AS AdGroupName,
    ANY_VALUE( Criteria                ) AS Criteria,
		ngram,
		Date,
		ARRAY_LENGTH( SPLIT( ngram, ' ' ) )            AS arity,
		SUM( EstimatedCost               )             AS EstimatedCost,
		SUM( GaSessions                  )             AS GaSessions,
		SUM( goal1Completions            )             AS goal1Completions,
		SUM( goal2Completions            )             AS goal2Completions,
		SUM( goal3Completions            )             AS goal3Completions,
		SUM( goal4Completions            )             AS goal4Completions,
		SUM( goal5Completions            )             AS goal5Completions,
		SUM( goal6Completions            )             AS goal6Completions,
		SUM( goal7Completions            )             AS goal7Completions,
		SUM( goal8Completions            )             AS goal8Completions,
		SUM( goal9Completions            )             AS goal9Completions,
		SUM( goal10Completions           )             AS goal10Completions,
		SUM( goal11Completions           )             AS goal11Completions,
		SUM( goal12Completions           )             AS goal12Completions,
		SUM( goal13Completions           )             AS goal13Completions,
		SUM( goal14Completions           )             AS goal14Completions,
		SUM( goal15Completions           )             AS goal15Completions,
		SUM( goal16Completions           )             AS goal16Completions,
		SUM( goal17Completions           )             AS goal17Completions,
		SUM( goal18Completions           )             AS goal18Completions,
		SUM( goal19Completions           )             AS goal19Completions,
		SUM( goal20Completions           )             AS goal20Completions,
		SUM( transactions                )             AS transactions,
		SUM( transactionRevenue          )             AS transactionRevenue,
	FROM ghostQueries, UNNEST( ML.NGRAMS( SPLIT( Query, ' ' ), [ 1, 3 ] ) ) AS ngram
	GROUP BY
		ExternalCustomerId,
    CampaignId,
    AdGroupId,
    CriterionId,
		ngram,
		Date
)
SELECT
	*
FROM ngrams`,
	},

	{ // missrouting_queries
		reference : BQ.MISSROUTING_QUERIES,
		query     : `WITH
sq AS (
	SELECT
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		sq.Query,
		sq.QueryMatchTypeWithVariant,
		sq.Date,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) / 1000000 AS Cost,
		ROUND( SUM( Conversions ) ) AS Conversions,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE TRUE
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE = AdGroup._LATEST_DATE
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
		AND Campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND AdGroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
		AND sq.QueryMatchTypeWithVariant IN ( 'PHRASE', 'NEAR_PHRASE', 'EXPANDED', 'BROAD', 'AUTO' ) --'EXACT', 'NEAR_EXACT' )
	GROUP BY
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date
),
AdgroupStats AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) / 1000000 AS Cost,
		ROUND( SUM( Conversions ) ) AS Conversions,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue
	FROM \`${ Object.values( BQ.AD_GROUP_STATS ).join( '.' ) }\`
	WHERE TRUE
		AND DATE_DIFF( _LATEST_DATE, _DATA_DATE, DAY ) <= 30
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId
),
keyword AS (
	SELECT
		Keyword.ExternalCustomerId,
		Keyword.Campaignid,
		Keyword.AdGroupid,
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		Keyword.Criteria
	FROM \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` Keyword
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE TRUE
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE = AdGroup._LATEST_DATE
		AND Keyword._DATA_DATE = Keyword._LATEST_DATE
		AND Campaign.CampaignStatus IN ( 'ENABLED' )
		AND AdGroup.AdGroupStatus   IN ( 'ENABLED' )
		AND Keyword.Status          IN ( 'ENABLED' )
		AND Keyword.KeywordMatchType = 'EXACT'
		AND NOT Keyword.IsNegative
	GROUP BY
		Keyword.ExternalCustomerId,
		Keyword.Campaignid,
		Keyword.AdGroupid,
		AccountName,
		CampaignName,
		AdGroupName,
		Criteria
),
sqAndKeyword AS (
	SELECT
		keyword.ExternalCustomerId,
		keyword.Campaignid,
		keyword.AdGroupid,
		sq.AccountName,
		sq.CampaignName,
		sq.AdGroupName,
		sq.Query,
		sq.QueryMatchTypeWithVariant,
		sq.Date,
		keyword.CampaignName AS KeywordCampaignName,
		keyword.AdGroupName AS KeywordAdGroupName,
		
		sq.Impressions AS Impressions,
		sq.Clicks AS Clicks,
		sq.Cost AS Cost,
		ROUND( sq.Conversions ) AS Conversions,
		ROUND( sq.ConversionValue ) AS ConversionValue,
		RANK() OVER (
			PARTITION BY
				sq.AccountName,
				sq.CampaignName,
				sq.AdGroupName,
				sq.Query,
				sq.QueryMatchTypeWithVariant,
				sq.Date
			ORDER BY
				AdGroupStats.Impressions 
				+ AdGroupStats.Clicks * 1000 
				+ CAST ( sq.CampaignName = keyword.CampaignName AS INT64 ) * 10000
				+ CAST ( sq.CampaignName = keyword.CampaignName AND sq.AdGroupName = keyword.AdGroupName AS INT64 ) * 1000000
				DESC
		) AS rank
	FROM sq
	JOIN keyword
		ON sq.AccountName = keyword.AccountName
		AND Query = LOWER( Criteria )
	JOIN AdgroupStats
		USING( ExternalCustomerId, CampaignId, AdGroupId )
),
proposed AS (
	SELECT
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		KeywordCampaignName AS ProposedKeywordCampaignName,
		KeywordAdGroupName AS ProposedKeywordAdGroupName,
		
		Impressions AS Impressions,
		Clicks AS Clicks,
		Cost AS Cost,
		ROUND( Conversions ) AS Conversions,
		ROUND( ConversionValue ) AS ConversionValue
	FROM
		sqAndKeyword
	WHERE TRUE
		AND rank = 1
),
sqKeywordAggr AS (
	SELECT
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		STRING_AGG( CONCAT( KeywordCampaignName, ' > ', KeywordAdGroupName ), ', ' ) AS KeywordAdGroups,
		Count(*) AS CountExactKeywords
	FROM
		sqAndKeyword
	WHERE TRUE
	GROUP BY
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date
)
SELECT
	AccountName,
	CampaignName,
	AdGroupName,
	Query,
	QueryMatchTypeWithVariant,
	Date,
	KeywordAdGroups,
	CountExactKeywords,
	ProposedKeywordCampaignName,
	ProposedKeywordAdGroupName,
	Impressions,
	Clicks,
	Cost,
	Conversions,
	ConversionValue
FROM
	proposed
JOIN sqKeywordAggr
	USING( 
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date
	)
WHERE TRUE`,
	},
	{ // missing_dsa_keywords
		reference : BQ.MISSING_DSA_KEYWORDS,
		query     : `WITH
keyword AS (
  SELECT
    ExternalCustomerId,
		Keyword.Criteria
	FROM \`${ Object.values( BQ.KEYWORD  ).join( '.' ) }\` Keyword
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE true
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE  = AdGroup._LATEST_DATE
		AND Keyword._DATA_DATE  = Keyword._LATEST_DATE
		AND Status          IN ( 'ENABLED' )
		AND CampaignStatus  IN ( 'ENABLED' )
		AND AdGroupStatus   IN ( 'ENABLED' )
		AND Keyword.KeywordMatchType = 'EXACT'
		AND NOT Keyword.IsNegative
	GROUP BY
    ExternalCustomerId,
		Criteria
),
sq AS (
	SELECT
    ExternalCustomerId,
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		sq.Query,
		sq.QueryMatchTypeWithVariant,
		sq.Date,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) AS Cost,
		ROUND( SUM( Conversions ) ) AS Conversions,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE true
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE = AdGroup._LATEST_DATE
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
		AND Campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND AdGroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
		--AND sq.QueryMatchTypeWithVariant NOT IN ( 'EXACT', 'NEAR_EXACT' )
		AND AdGroup.AdGroupType = 'SEARCH_DYNAMIC_ADS'
	GROUP BY
    ExternalCustomerId,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date
),
result AS (
  SELECT
    AccountName,
    CampaignName,
    AdGroupName,
    Query,
    QueryMatchTypeWithVariant,
    Date,
    SUM( Impressions ) AS Impressions,
    SUM( Clicks ) AS Clicks,
    SUM( Cost ) / 1000000 AS Cost,
    ROUND( SUM( Conversions ) ) AS Conversions,
    ROUND( SUM( ConversionValue ) ) AS ConversionValue
  FROM sq
  LEFT JOIN keyword
    ON sq.Query = LOWER( keyword.Criteria )
    AND sq.ExternalCustomerId = keyword.ExternalCustomerId
  WHERE TRUE
    AND keyword.Criteria IS NULL
  GROUP BY
    AccountName,
    CampaignName,
    AdGroupName,
    Query,
    QueryMatchTypeWithVariant,
    Date
  ORDER BY
    Clicks DESC
),
match_types AS (
  SELECT
    QueryMatchTypeWithVariant,
    COUNT(*)    AS Count,
    min( Date ) AS start,
    max( Date ) AS stop,
  FROM sq
  GROUP BY
    QueryMatchTypeWithVariant
)
SELECT *
FROM result`,
	},
	{ // missing_keywords
		reference : BQ.MISSING_KEYWORDS,
		query     : `WITH
sq AS (
	SELECT
    ExternalCustomerId,
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		sq.Query,
		sq.QueryMatchTypeWithVariant,
		Keyword.Criteria,
		sq.Date,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) AS Cost,
		ROUND( SUM( Conversions ) ) AS Conversions,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	JOIN \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` Keyword
		USING ( ExternalCustomerId, CampaignId, AdGroupId, CriterionId )
	WHERE true
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE = AdGroup._LATEST_DATE
		AND Keyword._DATA_DATE = Keyword._LATEST_DATE
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
		AND Campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND AdGroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
		AND sq.QueryMatchTypeWithVariant NOT IN ( 'EXACT', 'NEAR_EXACT' )
	GROUP BY
    ExternalCustomerId,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Criteria,
		Date
),
keyword AS (
	SELECT
    ExternalCustomerId,
		Keyword.Criteria
	FROM \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` Keyword
	WHERE true
		AND Keyword._DATA_DATE = Keyword._LATEST_DATE
		AND Keyword.Status IN ( 'ENABLED', 'PAUSED' )
		AND Keyword.KeywordMatchType = 'EXACT'
		AND NOT Keyword.IsNegative
	GROUP BY
    ExternalCustomerId,
		Criteria
)
SELECT
	AccountName,
	CampaignName,
	AdGroupName,
	Query,
	QueryMatchTypeWithVariant,
	sq.Criteria,
	Date,
	SUM( Impressions ) AS Impressions,
	SUM( Clicks ) AS Clicks,
	SUM( Cost ) / 1000000 AS Cost,
	ROUND( SUM( Conversions ) ) AS Conversions,
	ROUND( SUM( ConversionValue ) ) AS ConversionValue
FROM sq
LEFT JOIN keyword
	ON sq.Query = LOWER( keyword.Criteria )
  AND sq.ExternalCustomerId = keyword.ExternalCustomerId
WHERE TRUE
	AND keyword.Criteria IS NULL
GROUP BY
	AccountName,
	CampaignName,
	AdGroupName,
	Query,
	QueryMatchTypeWithVariant,
	sq.Criteria,
	Date
ORDER BY
	Clicks DESC`,
	},
	{ // paused_keywords
		reference : BQ.PAUSED_KEYWORDS,
		query     : `WITH
sq AS (
	SELECT
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		sq.Query,
		sq.QueryMatchTypeWithVariant,
		sq.Date,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) AS Cost,
		ROUND( SUM( Conversions ) ) AS Conversions,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE TRUE
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE = AdGroup._LATEST_DATE
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
		AND Campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND AdGroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
		AND sq.QueryMatchTypeWithVariant IN ( 'PHRASE', 'NEAR_PHRASE', 'EXPANDED', 'BROAD', 'AUTO') --'EXACT', 'NEAR_EXACT' )
	GROUP BY
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date
),
keyword AS (
	SELECT
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		Keyword.Criteria
	FROM \`${ Object.values( BQ.KEYWORD  ).join( '.' ) }\` Keyword
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE TRUE
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE  = AdGroup._LATEST_DATE
		AND Keyword._DATA_DATE  = Keyword._LATEST_DATE
		AND
			(
				Campaign.CampaignStatus  IN ( 'PAUSED' )
				OR AdGroup.AdGroupStatus IN ( 'PAUSED' )
				OR Keyword.Status        IN ( 'PAUSED' )
			)
		AND Campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND AdGroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
		AND Keyword.Status          IN ( 'ENABLED', 'PAUSED' )
		AND Keyword.KeywordMatchType = 'EXACT'
		AND NOT Keyword.IsNegative
	GROUP BY
		AccountName,
		CampaignName,
		AdGroupName,
		Criteria
),
keyword2 AS ( -- keyword2
	SELECT
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
		AdGroup.AdGroupName,
		Keyword.Criteria
	FROM \`${ Object.values( BQ.KEYWORD  ).join( '.' ) }\` Keyword
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE TRUE
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
		AND AdGroup._DATA_DATE  = AdGroup._LATEST_DATE
		AND Keyword._DATA_DATE  = Keyword._LATEST_DATE
		AND Campaign.CampaignStatus IN ( 'ENABLED' )
		AND AdGroup.AdGroupStatus   IN ( 'ENABLED' )
		AND Keyword.Status          IN ( 'ENABLED' )
		AND Keyword.KeywordMatchType = 'EXACT'
		AND NOT Keyword.IsNegative
	GROUP BY
		AccountName,
		CampaignName,
		AdGroupName,
		Criteria
)
SELECT
	sq.AccountName,
	sq.CampaignName,
	sq.AdGroupName,
	sq.Query,
	sq.QueryMatchTypeWithVariant,
	sq.Date,
	keyword.CampaignName            AS KeywordCampaignName,
	keyword.AdGroupName             AS KeywordAdGroupname,
	SUM( Impressions )              AS Impressions,
	SUM( Clicks )                   AS Clicks,
	SUM( Cost ) / 1000000           AS Cost,
	ROUND( SUM( Conversions ) )     AS Conversions,
	ROUND( SUM( ConversionValue ) ) AS ConversionValue
FROM sq
JOIN keyword
	ON sq.AccountName = keyword.AccountName
	AND Query = LOWER( Criteria )
LEFT JOIN keyword2
	ON sq.AccountName = keyword2.AccountName
	AND sq.Query = LOWER( keyword2.Criteria )
WHERE TRUE
	AND keyword2.Criteria IS NULL
GROUP BY
	AccountName,
	CampaignName,
	AdGroupName,
	Query,
	QueryMatchTypeWithVariant,
	Date,
	KeywordCampaignName,
	KeywordAdGroupName
ORDER BY
	Clicks DESC`,
	},
	{ // shopping_missing_keywords
		reference : BQ.SHOPPING_MISSING_KEYWORDS,
		query     : `WITH
sq AS (
	SELECT
    ExternalCustomerId,
		IFNULL( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Campaign.CampaignName,
    AdGroupName,
		sq.Query,
		sq.QueryMatchTypeWithVariant,
		sq.Date,
		SUM( Impressions ) AS Impressions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) AS Cost,
		ROUND( SUM( Conversions ) ) AS Conversions,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` Customer
		USING ( ExternalCustomerId )
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` Campaign
		USING ( ExternalCustomerId, CampaignId )
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AdGroup
		USING ( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE true
		AND Customer._DATA_DATE = Customer._LATEST_DATE
		AND Campaign._DATA_DATE = Campaign._LATEST_DATE
    AND AdGroup._DATA_DATE = AdGroup._LATEST_DATE
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
		AND Campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND AdGroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
		--AND sq.QueryMatchTypeWithVariant NOT IN ( 'EXACT', 'NEAR_EXACT' )
		AND Campaign.AdvertisingChannelType = 'SHOPPING'
	GROUP BY
    ExternalCustomerId,
		AccountName,
		CampaignName,
    AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date
),
keyword AS (
	SELECT
    ExternalCustomerId,
		Keyword.Criteria
	FROM \`${ Object.values( BQ.KEYWORD  ).join( '.' ) }\` Keyword
	WHERE true
		AND Keyword._DATA_DATE = Keyword._LATEST_DATE
		AND Keyword.Status IN ( 'ENABLED' )
		AND Keyword.KeywordMatchType = 'EXACT'
		AND NOT Keyword.IsNegative
	GROUP BY
    ExternalCustomerId,
		Criteria
),
result AS (
  SELECT
    AccountName,
    CampaignName,
    AdGroupName,
    Query,
    QueryMatchTypeWithVariant,
    Date,
    SUM( Impressions ) AS Impressions,
    SUM( Clicks ) AS Clicks,
    SUM( Cost ) / 1000000 AS Cost,
    ROUND( SUM( Conversions ) ) AS Conversions,
    ROUND( SUM( ConversionValue ) ) AS ConversionValue
  FROM sq
  LEFT JOIN keyword
    ON TRUE
      AND sq.Query = LOWER( keyword.Criteria )
      AND sq.ExternalCustomerId = keyword.ExternalCustomerId
  WHERE TRUE
    AND keyword.Criteria IS NULL
  GROUP BY
    AccountName,
    CampaignName,
    AdGroupName,
    Query,
    QueryMatchTypeWithVariant,
    Date
  ORDER BY
    Clicks DESC
)
SELECT
  *
FROM result`,
	},
	{ // routing_problems
		reference : BQ.ROUTING_PROBLEMS,
		query     : `WITH
paused_keywords AS (
	SELECT
		'paused_keywords' AS Type,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		CAST( NULL AS STRING ) AS KeywordAdGroups,
		CAST( NULL AS INT64  ) AS CountExactKeywords,
		KeywordCampaignName,
		KeywordAdGroupname,
		Impressions,
		Clicks,
		Cost,
		Conversions,
		ConversionValue,
	FROM \`${ Object.values( BQ.PAUSED_KEYWORDS ).join( '.' ) }\`
),
missing_keywords AS (
	SELECT
		'missing_keywords' AS Type,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		CAST( NULL AS STRING ) AS KeywordAdGroups,
		CAST( NULL AS INT64  ) AS CountExactKeywords,
		CAST( NULL AS STRING ) AS KeywordCampaignName,
		CAST( NULL AS STRING ) AS KeywordAdGroupname,
		Impressions,
		Clicks,
		Cost,
		Conversions,
		ConversionValue,
	FROM \`${ Object.values( BQ.MISSING_KEYWORDS ).join( '.' ) }\`
),
missrouting_queries AS (
	SELECT
		'missrouting_queries' AS Type,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		KeywordAdGroups, --
		CountExactKeywords, --
		ProposedKeywordCampaignName AS KeywordCampaignName,
		ProposedKeywordAdGroupName  AS KeywordAdGroupname,
		Impressions,
		Clicks,
		Cost,
		Conversions,
		ConversionValue,
	FROM \`${ Object.values( BQ.MISSROUTING_QUERIES ).join( '.' ) }\`
),
missing_dsa_keywords AS (
	SELECT
		'missing_dsa_keywords' AS Type,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		CAST( NULL AS STRING ) AS KeywordAdGroups,
		CAST( NULL AS Int64  ) AS CountExactKeywords,
		CAST( NULL AS STRING ) AS KeywordCampaignName,
		CAST( NULL AS STRING ) AS KeywordAdGroupname,
		Impressions,
		Clicks,
		Cost,
		Conversions,
		ConversionValue,
	FROM \`${ Object.values( BQ.MISSING_DSA_KEYWORDS ).join( '.' ) }\`
),
shopping_missing_keywords AS (
	SELECT
		'shopping_missing_keywords' AS Type,
		AccountName,
		CampaignName,
		AdGroupName,
		Query,
		QueryMatchTypeWithVariant,
		Date,
		CAST( NULL AS STRING ) AS KeywordAdGroups,
		CAST( NULL AS Int64  ) AS CountExactKeywords,
		CAST( NULL AS STRING ) AS KeywordCampaignName,
		CAST( NULL AS STRING ) AS KeywordAdGroupname,
		Impressions,
		Clicks,
		Cost,
		Conversions,
		ConversionValue,
	FROM \`${ Object.values( BQ.SHOPPING_MISSING_KEYWORDS ).join( '.' ) }\`
)
SELECT
  *
FROM paused_keywords
UNION ALL
SELECT
  *
FROM missing_keywords
UNION ALL
SELECT
  *
FROM missrouting_queries
UNION ALL
SELECT
  *
FROM missing_dsa_keywords
UNION ALL
SELECT
  *
FROM shopping_missing_keywords
`,
	},
	{ // counts
		reference : BQ.COUNTS,
		query     : `WITH
missingKeywords AS (
	SELECT
		AccountName,
		Date,
		COUNT(*) AS CountUniqueMissingKeywords
	FROM (
		SELECT
			AccountName,
			Query,
			Date
		FROM \`${ Object.values( BQ.MISSING_KEYWORDS ).join( '.' ) }\`
		GROUP BY
			AccountName,
			Query,
			Date
	)
	GROUP BY
		AccountName,
		Date
),
misroutingQueries AS (
	SELECT
		AccountName,
		Date,
		COUNT(*) AS CountUniqueMisroutingQueries
	FROM (
		SELECT
			AccountName,
			Query,
			Date
		FROM \`${ Object.values( BQ.MISSROUTING_QUERIES ).join( '.' ) }\`
		GROUP BY
			AccountName,
			Query,
			Date
	)
	GROUP BY
		AccountName,
		Date
),
pausedKeywords AS (
	SELECT
		AccountName,
		Date,
		COUNT(*) AS CountUniquePausedKeywords
	FROM (
		SELECT
			AccountName,
			Query,
			Date,
		FROM \`${ Object.values( BQ.PAUSED_KEYWORDS ).join( '.' ) }\`
		GROUP BY
			AccountName,
			Query,
			Date
	)
	GROUP BY
		AccountName,
		Date
)
,
shoppingMissingKeywords AS (
	SELECT
		AccountName,
		Date,
		COUNT(*) AS CountUniqueShoppingMissingKeywords
	FROM (
		SELECT
			AccountName,
			Query,
			Date
		FROM \`${ Object.values( BQ.SHOPPING_MISSING_KEYWORDS ).join( '.' ) }\`
		GROUP BY
			AccountName,
			Query,
			Date
	)
	GROUP BY
		AccountName,
		Date
)
,
dsaMissingKeywords AS (
	SELECT
		AccountName,
		Date,
		COUNT(*) AS CountUniqueDsaMissingKeywords
	FROM (
		SELECT
			AccountName,
			Query,
			Date
		FROM \`${ Object.values( BQ.MISSING_DSA_KEYWORDS ).join( '.' ) }\`
		GROUP BY
			AccountName,
			Query,
			Date
	)
	GROUP BY
		AccountName,
		Date
)
SELECT
	AccountName,
	Date,
	CountUniqueMissingKeywords,
	CountUniqueMisroutingQueries,
	CountUniquePausedKeywords,
	CountUniqueShoppingMissingKeywords,
	CountUniqueDsaMissingKeywords
FROM
	missingKeywords
LEFT JOIN misroutingQueries
	USING( AccountName, Date )
LEFT JOIN pausedKeywords
	USING( AccountName, Date )
LEFT JOIN shoppingMissingKeywords
	USING( AccountName, Date )
LEFT JOIN dsaMissingKeywords
	USING( AccountName, Date )
ORDER BY
	AccountName,
	Date`,
	},
	{ // ngram
		reference : BQ.NGRAM,
		query     : `WITH
sq AS (
	SELECT
		sq.ExternalCustomerId,
		sq.CampaignId,
		sq.AdGroupId,
		sq.CriterionId,
		ANY_VALUE( CustomerDescriptiveName ) AS CustomerDescriptiveName,
		ANY_VALUE( CampaignName            ) AS CampaignName,
		ANY_VALUE( AdGroupName             ) AS AdGroupName,
		ANY_VALUE( Criteria                ) AS Criteria,
		Query,
		Date,
		SUM( Impressions                  ) AS Impressions,
		SUM( Clicks                       ) AS Clicks,
		SUM( Cost                         ) AS Cost,
		SUM( Conversions                  ) AS Conversions,
		SUM( ConversionValue              ) AS ConversionValue,
		LENGTH( Query                     ) AS QueryLength,
		ARRAY_LENGTH( SPLIT( Query, ' ' ) ) AS CountWords
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` sq
	LEFT JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` customer
		ON TRUE
		AND sq.ExternalCustomerId = customer.ExternalCustomerId
		AND customer._DATA_DATE   = customer._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` campaign
		ON TRUE
		AND sq.ExternalCustomerId = campaign.ExternalCustomerId
		AND sq.CampaignId         = campaign.CampaignId
		AND campaign._DATA_DATE   = campaign._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` adGroup
		ON TRUE
		AND sq.ExternalCustomerId = adGroup.ExternalCustomerId
		AND sq.CampaignId         = adGroup.CampaignId
		AND sq.AdGroupId          = adGroup.AdGroupId
		AND adGroup._DATA_DATE    = adGroup._LATEST_DATE
	LEFT JOIN \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` keyword
		ON TRUE
		AND sq.ExternalCustomerId = keyword.ExternalCustomerId
		AND sq.CampaignId         = keyword.CampaignId
		AND sq.AdGroupId          = keyword.AdGroupId
		AND sq.CriterionId        = keyword.CriterionId
		AND keyword._DATA_DATE    = keyword._LATEST_DATE
  WHERE TRUE
    AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
    AND customer._DATA_DATE = customer._LATEST_DATE
  GROUP BY
    ExternalCustomerId,
    sq.CampaignId,
    sq.AdGroupId,
    sq.CriterionId,
    Date,
    Query
),
ngram AS (
	SELECT
		ExternalCustomerId,
    sq.CampaignId,
    sq.AdGroupId,
    sq.CriterionId,
    ANY_VALUE( CustomerDescriptiveName ) AS CustomerDescriptiveName,
    ANY_VALUE( CampaignName            ) AS CampaignName,
    ANY_VALUE( AdGroupName             ) AS AdGroupName,
    ANY_VALUE( Criteria                ) AS Criteria,
		Date,
		Ngram,
    SUM( Impressions          )           AS Impressions,
		SUM( Clicks               )           AS Clicks,
		SUM( Cost ) / 1000000                 AS Cost,
		ROUND( SUM( Conversions ) )           AS Conversions,
		ROUND( SUM( ConversionValue ) )       AS ConversionValue,
    
		ROUND( AVG( QueryLength ) * 10 ) / 10 AS AvgQueryLength,
		ROUND( AVG( CountWords  ) * 10 ) / 10 AS AvgCountWords,
		COUNT(*)                              AS Count,
    ARRAY_LENGTH( SPLIT( Ngram, ' ' ) )   AS Arity,
	FROM sq, UNNEST( ML.NGRAMS( SPLIT( Query, ' ' ), [ 1, 3 ] ) ) AS Ngram
	GROUP BY
		ExternalCustomerId,
    sq.CampaignId,
    sq.AdGroupId,
    sq.CriterionId,
		Date,
		Ngram
	HAVING TRUE
		--AND LENGTH( Ngram ) > 2
		--AND Count > 1
)
SELECT * FROM ngram`,
	},
	{ // raw_data
		reference : BQ.RAW_DATA,
		query     : `WITH
analytics AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		ROUND( AVG( AveragePageViews ) ) AS AveragePageViews,
		ROUND( AVG( AverageTimeOnSite ) ) AS AverageTimeOnSite,
		ROUND( AVG( BounceRate ) * 100 ) / 100 AS BounceRate,
		Date
	FROM \`${ Object.values( BQ.KEYWORD_CROSS_DEVICE_STATS ).join( '.' ) }\`
	
	WHERE TRUE
		AND DATE_DIFF( _LATEST_DATE, _DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Date
),
keyword AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		ROUND( SUM( Conversions ) ) AS Conversions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) / 1000000 AS Cost,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue,
		Date,
		AdNetworkType1,
		Device
	FROM \`${ Object.values( BQ.KEYWORD_BASIC_STATS ).join( '.' ) }\`
	WHERE TRUE
		AND DATE_DIFF( _LATEST_DATE, _DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Date,
		AdNetworkType1,
		Device
),
ngram AS (
	SELECT
		ExternalCustomerId,
		Word1 AS Word,
		ROUND( SUM( Conversions ) ) AS Conversions,
		SUM( Clicks ) AS Clicks,
		SUM( Cost ) / 1000000 AS Cost,
		ROUND( SUM( ConversionValue ) ) AS ConversionValue,
		COUNT(*) as Count,
		Date
	FROM (
		SELECT
			ExternalCustomerId,
			Query,
			SUM( Conversions ) AS Conversions,
			SUM( Clicks ) AS Clicks,
			SUM( Cost ) AS Cost,
			SUM( ConversionValue) AS ConversionValue,
			SPLIT( Query, ' ' ) as Word,
			Date
		FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` AS sq
		WHERE TRUE
			AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
		GROUP BY
			ExternalCustomerId,
			Query,
			Date
	) as x,
	UNNEST( Word ) as Word1
	GROUP BY
		ExternalCustomerId,
		Word1,
		Date
	HAVING
		Count >= 10
		AND LENGTH( Word ) > 2
),
sq AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Query,
		AdNetworkType1, -- attribute ( SEARCH / CONTENT / YOUTUBE_SEARCH / .. )
		--AdFormat, -- segment ( text / image / ... )
		Device, -- segment ( desktop / tablet / connected_tv / .. )
		QueryMatchTypeWithVariant,
			-- segment ( auto / broad / exact / expanded / phrase / near_exact / near_phrase
		QueryTargetingStatus, -- attribute ( added / excluded / both / none )
		ROUND( SUM( sq.Conversions ) ) AS Conversions,
		SUM( sq.Clicks ) AS Clicks,
		SUM( sq.Cost ) / 1000000 AS Cost,
		ROUND( SUM( sq.ConversionValue ) ) AS ConversionValue,
		Date
	FROM \`${ Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) }\` AS sq
	WHERE
		TRUE
		--AND LENGTH( Query ) < 80
		AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= ${MAX_DAYS_OF_GOOGLE_ADS_DATA}
	GROUP BY
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Query,
		AdNetworkType1,
		--AdFormat,
		Device,
		QueryMatchTypeWithVariant,
		QueryTargetingStatus,
		Date
	--HAVING TRUE
		--AND Clicks > 10
),
query1 AS (
	SELECT
		customer.ExternalCustomerId,
		sq.CampaignId,
		sq.AdGroupId,
		--sq.CriterionId,
		ifnull( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
		Query,
		campaign.CampaignName,
		adgroup.AdGroupName,
		keyword1.Criteria,
		sq.Conversions AS Conversions,
		sq.Clicks AS Clicks,
		sq.Cost AS Cost,
		sq.ConversionValue AS ConversionValue,
		keyword.Conversions AS KeywordConversions,
		keyword.Clicks AS KeywordClicks,
		keyword.Cost AS KeywordCost,
		keyword.ConversionValue AS KeywordConversionValue,
		AveragePageViews AS AveragePageViews,
		AverageTimeOnSite AS AverageTimeOnSite,
		BounceRate AS BounceRate,
		keyword1.QualityScore AS QualityScore,
		sq.AdNetworkType1,
		--AdFormat,
		sq.Device,
		sq.QueryMatchTypeWithVariant,
		sq.QueryTargetingStatus,
		sq.Date AS Date
	FROM sq
	JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` AS customer
		ON customer.ExternalCustomerId = sq.ExternalCustomerId
	JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` AS campaign
		ON campaign.ExternalCustomerId = sq.ExternalCustomerId
		AND sq.CampaignId = campaign.CampaignId
	JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` AS adgroup
		ON adgroup.ExternalCustomerId = sq.ExternalCustomerId
		AND sq.CampaignId = adgroup.CampaignId
		AND sq.AdGroupId = adgroup.AdGroupId
	JOIN \`${ Object.values( BQ.KEYWORD ).join( '.' ) }\` AS keyword1
		ON keyword1.ExternalCustomerId = sq.ExternalCustomerId
		AND keyword1.CampaignId = sq.CampaignId
		AND keyword1.AdGroupId = sq.AdGroupId
		AND keyword1.CriterionId = sq.CriterionId
	JOIN analytics
		ON analytics.ExternalCustomerId = sq.ExternalCustomerId
		AND analytics.CampaignId = sq.CampaignId
		AND analytics.AdGroupId = sq.AdGroupId
		AND analytics.CriterionId = sq.CriterionId
		AND analytics.Date = sq.Date
	LEFT JOIN keyword
		ON keyword.ExternalCustomerId = sq.ExternalCustomerId
		AND keyword.CampaignId = sq.CampaignId
		AND keyword.AdGroupId = sq.AdGroupId
		AND keyword.CriterionId = sq.CriterionId
		AND keyword.Date = sq.Date
		AND keyword.AdNetworkType1 = sq.AdNetworkType1
		AND keyword.Device = sq.Device
		AND keyword1.KeywordMatchType = sq.QueryMatchTypeWithVariant
	WHERE
		TRUE
		AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
		AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
		AND keyword1.Status IN ( 'ENABLED', 'PAUSED' )
		AND NOT keyword1.IsNegative
		AND customer._LATEST_DATE = customer._DATA_DATE
		AND campaign._LATEST_DATE = campaign._DATA_DATE
		AND adgroup._LATEST_DATE = adgroup._DATA_DATE
		AND keyword1._LATEST_DATE = keyword1._DATA_DATE
),
query2 AS (
	SELECT
		query1.ExternalCustomerId,
		query1.CampaignId,
		query1.AdGroupId,
		query1.AccountName,
		query1.CampaignName,
		query1.AdGroupName,
		query1.Criteria,
		query1.Query,
		ROUND( ANY_VALUE( query1.Conversions ) ) AS QueryConversions,
		ANY_VALUE( query1.Clicks ) AS QueryClicks,
		ROUND( ANY_VALUE( query1.Cost ) * 100 ) / 100 AS QueryCost,
		ROUND( ANY_VALUE( query1.ConversionValue ) ) AS QueryConversionValue,
		ROUND( 
			( ANY_VALUE( query1.Conversions ) * 50 - ANY_VALUE( query1.Cost ) )
			/ GREATEST( ANY_VALUE( query1.Clicks ), 1 ) * 100 ) / 100 AS QueryProfit_per_click,
			
		ROUND( SUM( ngram.Conversions ) ) AS NgramConversions,
		SUM( ngram.Clicks ) AS NgramClicks,
		ROUND( SUM( ngram.Cost ) * 100 ) / 100 AS NgramCost,
		ROUND( SUM( ngram.ConversionValue ) ) AS NgramConversionValue,
		ROUND( ( SUM( ngram.Conversions ) * 50 - SUM( ngram.Cost ) ) 
			/ GREATEST( SUM( ngram.Clicks ), 1 ) * 100 ) / 100 AS NgramProfit_per_click,
		
		ROUND( ANY_VALUE( KeywordConversions ) ) AS KeywordConversions,
		ANY_VALUE( KeywordClicks ) AS KeywordClicks,
		ROUND( ANY_VALUE( KeywordCost ) * 100 ) / 100 AS KeywordCost,
		ROUND( ANY_VALUE( KeywordConversionValue ) ) AS KeywordConversionValue,
		ROUND( ( ANY_VALUE( KeywordConversions ) * 50 - ANY_VALUE( KeywordCost ) ) 
			/ GREATEST( ANY_VALUE( KeywordClicks ), 1 ) * 100 ) / 100 AS KeywordProfit_per_click,
		
		ROUND( ANY_VALUE( AveragePageViews ) ) AS AveragePageViews,
		ROUND( ANY_VALUE( AverageTimeOnSite ) ) AS AverageTimeOnSite,
		ROUND( ANY_VALUE( BounceRate ) * 100 ) / 100 AS BounceRate,
		
		ANY_VALUE( query1.QualityScore ) AS QualityScore,
		LENGTH( query1.Query ) AS QueryLength,
		ARRAY_LENGTH( SPLIT( query1.Query, ' ' ) ) AS CountWords,
		
		AdNetworkType1 AS AdNetworkType1,
		--AdFormat AS AdFormat,
		Device AS Device,
		QueryMatchTypeWithVariant AS QueryMatchTypeWithVariant,
		QueryTargetingStatus AS QueryTargetingStatus,
		query1.Date
	FROM query1
	LEFT JOIN ngram
		ON TRUE
		AND ngram.ExternalCustomerId = query1.ExternalCustomerId
		AND REGEXP_CONTAINS(
			CONCAT( ' ', Query, ' ' ),
			CONCAT( ' ', REGEXP_REPLACE( Word, r'[\\[\\]"\\$\\*\\+\\?]', '' ), ' ' )
		)
		AND query1.Date = ngram.Date
	GROUP BY
		query1.ExternalCustomerId,
		query1.CampaignId,
		query1.AdGroupId,
		AccountName,
		CampaignName,
		AdGroupName,
		Criteria,
		Query,
		AdNetworkType1,
		Device,
		QueryMatchTypeWithVariant,
		QueryTargetingStatus,
		Date
),
adgroup_negative AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		Criteria
	FROM \`${ Object.values( BQ.CRITERIA ).join( '.' ) }\` AS negative
	WHERE TRUE
		AND negative.isNegative
		AND negative._LATEST_DATE = negative._DATA_DATE
),
query3 AS (
	SELECT
		query2.ExternalCustomerId,
		query2.CampaignId,
		query2.AdGroupId,
		query2.AccountName,
		query2.CampaignName,
		query2.AdGroupName,
		query2.Criteria,
		query2.Query,
		query2.AdNetworkType1,
		query2.Device,
		query2.QueryMatchTypeWithVariant,
		query2.QueryTargetingStatus,
		query2.Date,
		
		query2.QueryConversions,
		query2.QueryClicks,
		query2.QueryCost,
		query2.QueryConversionValue,
		query2.QueryProfit_per_click,
		
		query2.NgramConversions,
		query2.NgramClicks,
		query2.NgramCost,
		query2.NgramConversionValue,
		query2.NgramProfit_per_click,
			
		query2.KeywordConversions,
		query2.KeywordClicks,
		query2.KeywordCost,
		query2.KeywordConversionValue,
		query2.KeywordProfit_per_click,
		
		query2.AveragePageViews,
		query2.AverageTimeOnSite,
		query2.BounceRate,
		
		query2.QualityScore,
		query2.QueryLength,
		query2.CountWords,
	FROM
		query2
	GROUP BY
		query2.ExternalCustomerId,
		query2.CampaignId,
		query2.AdGroupId,
		query2.AccountName,
		query2.CampaignName,
		query2.AdGroupName,
		query2.Criteria,
		query2.Query,
		query2.AdNetworkType1,
		query2.Device,
		query2.QueryMatchTypeWithVariant,
		query2.QueryTargetingStatus,
		query2.Date,
		
		query2.QueryConversions,
		query2.QueryClicks,
		query2.QueryCost,
		query2.QueryConversionValue,
		query2.QueryProfit_per_click,
		
		query2.NgramConversions,
		query2.NgramClicks,
		query2.NgramCost,
		query2.NgramConversionValue,
		query2.NgramProfit_per_click,
			
		query2.KeywordConversions,
		query2.KeywordClicks,
		query2.KeywordCost,
		query2.KeywordConversionValue,
		query2.KeywordProfit_per_click,
		
		query2.AveragePageViews,
		query2.AverageTimeOnSite,
		query2.BounceRate,
		
		query2.QualityScore,
		query2.QueryLength,
		query2.CountWords
),
query4 AS (
	SELECT
		query3.ExternalCustomerId,
		query3.CampaignId,
		query3.AdGroupId,
		query3.AccountName,
		query3.CampaignName,
		query3.AdGroupName,
		query3.Criteria,
		query3.Query,
		query3.AdNetworkType1,
		query3.Device,
		query3.QueryMatchTypeWithVariant,
		query3.QueryTargetingStatus,
		query3.Date,
		
		query3.QueryConversions,
		query3.QueryClicks,
		query3.QueryCost,
		query3.QueryConversionValue,
		query3.QueryProfit_per_click,
		
		query3.NgramConversions,
		query3.NgramClicks,
		query3.NgramCost,
		query3.NgramConversionValue,
		query3.NgramProfit_per_click,
		
		query3.KeywordConversions,
		query3.KeywordClicks,
		query3.KeywordCost,
		query3.KeywordConversionValue,
		query3.KeywordProfit_per_click,
		
		query3.AveragePageViews,
		query3.AverageTimeOnSite,
		query3.BounceRate,
		
		query3.QualityScore,
		query3.QueryLength,
		query3.CountWords,
		
		COUNT( adgroup_negative.Criteria ) AS CountAdGroupNegatives
	FROM
		query3
	LEFT JOIN adgroup_negative
		ON  adgroup_negative.ExternalCustomerId = query3.ExternalCustomerId
		AND adgroup_negative.CampaignId = query3.CampaignId
		AND adgroup_negative.AdGroupId = query3.AdGroupId
		AND REGEXP_CONTAINS(
			CONCAT( ' ', LOWER( Query ), ' ' ),
			CONCAT(
				' ',
				REGEXP_REPLACE(
					LOWER( adgroup_negative.Criteria ),
					r'[\\[\\]"\\$\\*\\+\\?]',
					''
				),
				' '
			)
		)
	GROUP BY
		query3.ExternalCustomerId,
		query3.CampaignId,
		query3.AdGroupId,
		query3.AccountName,
		query3.CampaignName,
		query3.AdGroupName,
		query3.Criteria,
		query3.Query,
		query3.AdNetworkType1,
		query3.Device,
		query3.QueryMatchTypeWithVariant,
		query3.QueryTargetingStatus,
		query3.Date,
		
		query3.QueryConversions,
		query3.QueryClicks,
		query3.QueryCost,
		query3.QueryConversionValue,
		query3.QueryProfit_per_click,
		
		query3.NgramConversions,
		query3.NgramClicks,
		query3.NgramCost,
		query3.NgramConversionValue,
		query3.NgramProfit_per_click,
			
		query3.KeywordConversions,
		query3.KeywordClicks,
		query3.KeywordCost,
		query3.KeywordConversionValue,
		query3.KeywordProfit_per_click,
		
		query3.AveragePageViews,
		query3.AverageTimeOnSite,
		query3.BounceRate,
		
		query3.QualityScore,
		query3.QueryLength,
		query3.CountWords
)
SELECT *
FROM query4`,
	},
];

const COMMON_PITFALLS = {
	ACCOUNT_LIN_RODNITZKY : [
	`
	-- account_lin_rodnitzky
WITH
enumerator AS (
	SELECT
		ExternalCustomerId,
		SUM( Cost ) / 1000000 AS Cost,
		SUM( Conversions )    AS Conversions,
		SUM( Cost ) / 1000000 / NULLIF( SUM( Conversions ), 0 ) AS CPA,
	FROM ${ tsReference( 'SearchQueryStats'  ) } AS stats
	WHERE TRUE
		AND DATE_DIFF( CURRENT_DATE(), _DATA_DATE, DAY ) <= 30
	GROUP BY
		ExternalCustomerId
),
converted_sq AS (
	SELECT
		ExternalCustomerId,
		Query,
		SUM( Cost ) / 1000000 AS Cost,
		SUM( Conversions )    AS Conversions,
	FROM ${ tsReference( 'SearchQueryStats'  ) } AS stats
	WHERE TRUE
		AND DATE_DIFF( CURRENT_DATE(), _DATA_DATE, DAY ) <= 30
	GROUP BY
		ExternalCustomerId,
		Query
	HAVING TRUE
		AND SUM( stats.Conversions ) >= 1
),
denominator AS (
	SELECT
		ExternalCustomerId,
		ROUND( SUM( Cost ), 2 ) AS Cost,
		SUM( Conversions )      AS Conversions,
		SUM( Cost ) / NULLIF( SUM( Conversions ), 0 ) AS CPA,
	FROM converted_sq
	WHERE TRUE
	GROUP BY
		ExternalCustomerId
)
SELECT
	ExternalCustomerId,
	CustomerDescriptiveName AS AccountName,
	--ROUND( enumerator.CPA,  2 )   AS CPA_ALL,
	--ROUND( denominator.CPA, 2 )   AS CPA_Converted,
	--enumerator.Cost               AS Cost_ALL,
	--denominator.Cost              AS Cost_Converted,
	--enumerator.Conversions        AS Conversions_ALL,
	--denominator.Conversions       AS Conversions_Converted,
	ROUND( enumerator.CPA  / NULLIF( denominator.CPA,  0 ), 1 ) AS lin_rodnitzky,
	--ROUND( enumerator.Cost / NULLIF( denominator.Cost, 0 ), 1 ) AS tissen_riml,
FROM enumerator
JOIN denominator USING( ExternalCustomerId )
JOIN ${ tsReference( 'Customer'  ) } AS customer USING( ExternalCustomerId )
WHERE TRUE
  AND customer._DATA_DATE = customer._LATEST_DATE
ORDER BY
  lin_rodnitzky DESC
` ][ 0 ],
	ADGROUP_NO_KEYWORDS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND ( SELECT count(*) from `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword WHERE keyword.ExternalCustomerId = adgroup.ExternalCustomerId AND keyword.AdGroupId = adgroup.AdGroupId AND NOT IsNegative AND keyword._LATEST_DATE = keyword._DATA_DATE ) = 0',
		'	AND AdGroupType = \'SEARCH_STANDARD\'',
		'	AND AdvertisingChannelType = \'SEARCH\'',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_NO_ACTIVE_KEYWORDS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND ( SELECT count(*) from `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword WHERE keyword.ExternalCustomerId = adgroup.ExternalCustomerId AND keyword.AdGroupId = adgroup.AdGroupId AND NOT IsNegative AND Status = \'ENABLED\' AND keyword._LATEST_DATE = keyword._DATA_DATE ) = 0',
		'	AND ( SELECT count(*) from `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword WHERE keyword.AdGroupId = adgroup.AdGroupId AND NOT IsNegative AND keyword._LATEST_DATE = keyword._DATA_DATE ) > 0',
		'	AND AdGroupType = \'SEARCH_STANDARD\'',
		'	AND AdvertisingChannelType = \'SEARCH\'',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_TOO_MANY_EXACT_KEYWORDS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName,',
		//'	count(*) as countExactKeywords',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword ON keyword.ExternalCustomerId = adgroup.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId AND NOT IsNegative AND KeywordMatchType = \'EXACT\'',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND AdvertisingChannelType = \'SEARCH\'',
		'GROUP BY AccountName, CampaignName, AdGroupName, customer.ExternalCustomerId, CampaignId, AdGroupId, AdGroupStatus, CampaignStatus',
		'HAVING TRUE',
		'  AND count(*) > 10',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_NO_NEGATIVE_KEYWORDS_IN_BROAD_ADGROUP : [
		'SELECT',
		'    adgroup.AccountName,',
		'    CAST( adgroup.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'    CAST( adgroup.CampaignId as STRING ) as CampaignId,',
		'    CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'    adgroup.CampaignStatus,',
		'    adgroup.AdGroupStatus,',
		'    adgroup.CampaignName,',
		'    adgroup.AdGroupName',
		'    --,adgroup.CountBroadKeywords',
		'    --,SystemServingStatus',
		'    --,SUM( Impressions ) AS ImpressionisLast30Days',
		'FROM (',
		'  SELECT',
		'    IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'    customer.ExternalCustomerId,',
		'    campaign.CampaignId,',
		'    adgroup.AdGroupId,',
		'    campaign.CampaignStatus,',
		'    adgroup.AdGroupStatus,',
		'    CampaignName,',
		'    AdGroupName,',
		'    SUM( CAST( IsNegative AS INT64 ) ) AS CountNegativeKeywords,',
		'    SUM( CAST( NOT IsNegative AND KeywordMatchType != \'BROAD\' AS INT64 ) ) AS CountNonBroadKeywords,',
		'    SUM( CAST( NOT IsNegative AND KeywordMatchType = \'BROAD\' AS INT64 ) ) AS CountBroadKeywords,',
		'    STRING_AGG( SystemServingStatus, \', \' ) as SystemServingStatus',
		'  FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'  JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'  JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'  JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword ON keyword.ExternalCustomerId = customer.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'  WHERE TRUE',
		'    AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'    AND campaign.AdvertisingChannelType = \'SEARCH\'',
		'    AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'    AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'    AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'    AND customer._LATEST_DATE = customer._DATA_DATE',
		'    AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'    AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'  GROUP BY AccountName, ExternalCustomerId, CampaignName, campaign.CampaignId, AdGroupId, CampaignStatus, AdGroupStatus, CampaignName, AdGroupName',
		'  HAVING CountNegativeKeywords = 0 AND CountNonBroadKeywords = 0 AND CountBroadKeywords > 0',
		') AS adgroup',
		'-- the join excludes adgroups without impressions in the last 30 days',
		'JOIN `' + Object.values( BQ.KEYWORD_BASIC_STATS ).join( '.' ) + '` AS stat ON stat.ExternalCustomerId = adgroup.ExternalCustomerId AND stat.CampaignId = adgroup.CampaignId AND stat.AdGroupId = adgroup.AdGroupId AND DATE_DIFF( CURRENT_DATE( \'Europe/Berlin\' ), _DATA_DATE, DAY ) <= 30',
		'WHERE TRUE',
		'	AND REGEXP_CONTAINS( SystemServingStatus, \'ELIGIBLE\' )',
		'GROUP BY AccountName, ExternalCustomerId, CampaignName, adgroup.CampaignId, AdGroupId, CampaignStatus, AdGroupStatus, CampaignName, AdGroupName, CountBroadKeywords, SystemServingStatus',
		'ORDER BY AccountName, ExternalCustomerId, CampaignName, adgroup.CampaignId, AdGroupId, CampaignStatus, AdGroupStatus, CampaignName, AdGroupName, CountBroadKeywords, SystemServingStatus',
		'--ImpressionisLast30Days DESC',
	].join( '\n' ),
	ADGROUP_TOO_MANY_BROAD_KEYWORDS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName,',
		//'	count(*) as countBroadKeywords',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword ON keyword.ExternalCustomerId = adgroup.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId AND NOT IsNegative AND KeywordMatchType = \'BROAD\'',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND AdvertisingChannelType = \'SEARCH\'',
		'GROUP BY AccountName, CampaignName, AdGroupName, customer.ExternalCustomerId, CampaignId, AdGroupId, AdGroupStatus, CampaignStatus',
		'HAVING TRUE',
		'  AND count(*) > 1',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_NO_ADS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND (',
		'		SELECT',
		'			count(*) ',
		'		FROM `' + Object.values( BQ.AD ).join( '.' ) + '` AS ad',
		'		WHERE TRUE',
		'			AND ad.ExternalCustomerId = adgroup.ExternalCustomerId',
		'			AND ad.AdGroupId = adgroup.AdGroupId',
		'			AND AdType IN (',
		'				\'GMAIL_AD\',',
		'				\'MULTI_ASSET_RESPONSIVE_DISPLAY_AD\',',
		'				\'TEMPLATE_AD\',',
		'				\'CALL_ONLY_AD\',',
		'				\'EXPANDED_TEXT_AD\',',
		'				\'RESPONSIVE_SEARCH_AD\',',
		'				\'TEXT_AD\',',
		'				\'RESPONSIVE_DISPLAY_AD\',',
		'				\'IMAGE_AD\',',
		'				\'DYNAMIC_SEARCH_AD\',',
		'				\'EXPANDED_DYNAMIC_SEARCH_AD\'',
		'			)',
		'			AND ad._LATEST_DATE = ad._DATA_DATE',
		'	) = 0',
		'	AND AdGroupType IN ( \'SEARCH_STANDARD\', \'DISPLAY_STANDARD\' )',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_TOO_FEW_ADS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName,',
		'	count(*) AS countAds',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD ).join( '.' ) + '` AS ad',
		'	ON ad.ExternalCustomerId = adgroup.ExternalCustomerId',
		'	AND ad.CampaignId = campaign.CampaignId',
		'	AND ad.AdGroupId = adgroup.AdGroupId',
		'	AND AdType IN (',
		'		\'GMAIL_AD\',',
		'		\'MULTI_ASSET_RESPONSIVE_DISPLAY_AD\',',
		'		\'TEMPLATE_AD\',',
		'		\'CALL_ONLY_AD\',',
		'		\'EXPANDED_TEXT_AD\',',
		'		\'RESPONSIVE_SEARCH_AD\',',
		'		\'TEXT_AD\',',
		'		\'RESPONSIVE_DISPLAY_AD\',',
		'		\'IMAGE_AD\',',
		'		\'DYNAMIC_SEARCH_AD\',',
		'		\'EXPANDED_DYNAMIC_SEARCH_AD\'',
		'	)',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND AdGroupType IN ( \'SEARCH_STANDARD\', \'DISPLAY_STANDARD\' )',
		'GROUP BY AccountName, CampaignName, AdGroupName, CustomerDescriptiveName, AccountDescriptiveName, customer.ExternalCustomerId, AdGroupId, campaign.CampaignStatus, adgroup.AdGroupStatus, campaign.CampaignId',
		'HAVING TRUE',
		'	AND countAds = 2 --IN ( 1, 2, 3 )',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_TOO_MANY_ENABLED_ADS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName,',
		'	count(*) AS countAds',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD ).join( '.' ) + '` AS ad',
		'	ON ad.ExternalCustomerId = adgroup.ExternalCustomerId',
		'	AND ad.CampaignId = campaign.CampaignId',
		'	AND ad.AdGroupId = adgroup.AdGroupId',
		'	AND AdType IN (',
		'		\'GMAIL_AD\',',
		'		\'MULTI_ASSET_RESPONSIVE_DISPLAY_AD\',',
		'		\'TEMPLATE_AD\',',
		'		\'CALL_ONLY_AD\',',
		'		\'EXPANDED_TEXT_AD\',',
		'		\'RESPONSIVE_SEARCH_AD\',',
		'		\'TEXT_AD\',',
		'		\'RESPONSIVE_DISPLAY_AD\',',
		'		\'IMAGE_AD\',',
		'		\'DYNAMIC_SEARCH_AD\',',
		'		\'EXPANDED_DYNAMIC_SEARCH_AD\'',
		'	)',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad.Status IN ( \'ENABLED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND AdGroupType IN ( \'SEARCH_STANDARD\', \'DISPLAY_STANDARD\' )',
		'GROUP BY AccountName, CampaignName, AdGroupName, CustomerDescriptiveName, AccountDescriptiveName, customer.ExternalCustomerId, AdGroupId, campaign.CampaignStatus, adgroup.AdGroupStatus, campaign.CampaignId',
		'HAVING TRUE',
		'	AND countAds > 6',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_NO_DSA : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND (',
		'		SELECT',
		'			count(*)',
		'		FROM `' + Object.values( BQ.AD ).join( '.' ) + '` AS ad',
		'		WHERE TRUE',
		'			AND ad.ExternalCustomerId = adgroup.ExternalCustomerId',
		'			AND ad.AdGroupId = adgroup.AdGroupId',
		'			AND AdType IN (',
		'				\'EXPANDED_DYNAMIC_SEARCH_AD\',',
		'				\'DYNAMIC_SEARCH_AD\'',
		'			)',
		'			AND ad._LATEST_DATE = ad._DATA_DATE',
		'			AND Status = \'ENABLED\'',
		'	) = 0',
		'	AND AdGroupType = \'SEARCH_DYNAMIC_ADS\'',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_TOO_FEW_DSA : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND (',
		'		SELECT',
		'			count(*)',
		'		FROM `' + Object.values( BQ.AD ).join( '.' ) + '` AS ad',
		'		WHERE TRUE',
		'			AND ad.ExternalCustomerId = adgroup.ExternalCustomerId',
		'			AND ad.AdGroupId = adgroup.AdGroupId',
		'			AND AdType IN (',
		'				\'EXPANDED_DYNAMIC_SEARCH_AD\',',
		'				\'DYNAMIC_SEARCH_AD\'',
		'			)',
		'			AND ad._LATEST_DATE = ad._DATA_DATE',
		'			AND Status = \'ENABLED\'',
		'	) IN ( 1, 2 )',
		'	AND AdGroupType = \'SEARCH_DYNAMIC_ADS\'',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_TOO_MANY_DSA : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND (',
		'		SELECT',
		'			count(*)',
		'		FROM `' + Object.values( BQ.AD ).join( '.' ) + '` AS ad',
		'		WHERE TRUE',
		'			AND ad.ExternalCustomerId = adgroup.ExternalCustomerId',
		'			AND ad.AdGroupId = adgroup.AdGroupId',
		'			AND AdType IN (',
		'				\'EXPANDED_DYNAMIC_SEARCH_AD\',',
		'				\'DYNAMIC_SEARCH_AD\'',
		'			)',
		'			AND ad._LATEST_DATE = ad._DATA_DATE',
		'			AND Status = \'ENABLED\'',
		'	) > 6',
		'	AND AdGroupType = \'SEARCH_DYNAMIC_ADS\'',
		'ORDER BY AccountName, CampaignName, AdGroupName',
	].join( '\n' ),
	ADGROUP_WITHOUT_AUDIENCE : [
		'#StandardSQL',
		'SELECT',
		'	CustomerDescriptiveName as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignName,',
		'	adgroup.AdGroupName,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus',
		'	--,sum( cast( audience.ExternalCustomerId is not null as int64 ) ) as countAudiences',
		'FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` AS adgroup',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer',
		'	ON customer.ExternalCustomerId = adgroup.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'	ON campaign.CampaignId = adgroup.CampaignId',
		'	AND campaign.ExternalCustomerId = adgroup.ExternalCustomerId',
		'	AND campaign. AdvertisingChannelType IN ( \'SEARCH\', \'SHOPPING\' )',
		'	AND REGEXP_CONTAINS( campaign.CampaignName , \'RLSA\' )',
		'LEFT JOIN `' + Object.values( BQ.CRITERIA ).join( '.' ) + '` AS audience',
		'	ON  adgroup.ExternalCustomerId = audience.ExternalCustomerId',
		'	AND campaign.CampaignId = audience.CampaignId',
		'	AND adgroup.AdGroupId = audience.AdGroupId',
		'	AND audience._DATA_DATE = audience._LATEST_DATE',
		'	AND CriteriaType = \'USER_LIST\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND adgroup._DATA_DATE = adgroup._LATEST_DATE',
		'	AND audience.ExternalCustomerId is null',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	CustomerDescriptiveName,',
		'	campaign.CampaignName,',
		'	adgroup.AdGroupName,',
		'	CampaignId,',
		'	AdGroupId,',
		'	adgroup.AdGroupStatus,',
		'	campaign.CampaignStatus',
		'HAVING TRUE',
		'ORDER BY CustomerDescriptiveName, campaign.CampaignName, adgroup.AdGroupName',
	].join( '\n' ),
	AD_DUPLICATE_SPECIAL_CHARS_IN_DESCRIPTION : [
	`
		SELECT
			ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
			CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
			CAST( campaign.CampaignId as STRING ) as CampaignId,
			CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
			CampaignName,
			AdGroupName,
			campaign.CampaignStatus,
			AdGroupStatus,
			Status,
			HeadLinePart1,
			HeadLinePart2,
			Description,
			PolicySummary,
			Path1,
			Path2,
		FROM \`${ Object.values( BQ.AD       ).join( '.' ) }\` as ad
		JOIN \`${ Object.values( BQ.CAMPAIGN ).join( '.' ) }\` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId
		JOIN \`${ Object.values( BQ.CUSTOMER ).join( '.' ) }\` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId
		JOIN \`${ Object.values( BQ.AD_GROUP ).join( '.' ) }\` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId
		WHERE TRUE
			AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
			AND AdType = 'EXPANDED_TEXT_AD'
			AND Status IN ( 'ENABLED', 'PAUSED' )
			AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
			AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
			AND ad._LATEST_DATE = ad._DATA_DATE
			AND campaign._LATEST_DATE = campaign._DATA_DATE
			AND customer._LATEST_DATE = customer._DATA_DATE
			AND adgroup._LATEST_DATE = adgroup._DATA_DATE
			AND REGEXP_CONTAINS( Description, r'[\?\!].*[\?\!]' )
	`][ 0 ],
	AD_POLICY_VIOLATION : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'  	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CAST( ad.CreativeId as STRING ) as AdId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	Status,',
		'	HeadLinePart1,',
		'	HeadLinePart2,',
		'	Description,',
		'	PolicySummary,',
		'	Path1,',
		'	Path2',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND AdType = \'EXPANDED_TEXT_AD\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND PolicySummary is not null',
	].join( '\n' ),
	AD_PATH1_MISSING_IN_NON_BRAND_CAMPAIGN : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'  	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CAST( ad.CreativeId as STRING ) as AdId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	Status,',
		'	HeadLinePart1,',
		'	HeadLinePart2,',
		'	Description,',
		'	PolicySummary,',
		'	Path1,',
		'	Path2',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND AdType = \'EXPANDED_TEXT_AD\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND Path1 is null',
		'	AND NOT REGEXP_CONTAINS( CampaignName, \'Brand\' )',
	].join( '\n' ),
	AD_PATH2_MISSING_IN_NON_BRAND_CAMPAIGN : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CAST( ad.CreativeId as STRING ) as AdId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	AdGroupStatus,',
		'	Status,',
		'	HeadLinePart1,',
		'	HeadLinePart2,',
		'	Description,',
		'	PolicySummary,',
		'	Path1,',
		'	Path2',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND AdType = \'EXPANDED_TEXT_AD\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND Path2 is null',
		'	AND NOT REGEXP_CONTAINS( CampaignName, \'Brand\' )',
	].join( '\n' ),
	AD_TOO_SHORT_HEADLINE : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CAST( ad.CreativeId as STRING ) as AdId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	AdGroupStatus,',
		'	Status,',
		'	HeadLinePart1,',
		'	HeadLinePart2,',
		'	Description,',
		'	PolicySummary,',
		'	Path1,',
		'	Path2',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND AdType = \'EXPANDED_TEXT_AD\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND ( LENGTH( HeadLinePart1 ) <= 15 OR LENGTH( HeadLinePart2 ) <= 15 )',
	].join( '\n' ),
	AD_TOO_SHORT_DESCRIPTION : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CAST( ad.CreativeId as STRING ) as AdId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	AdGroupStatus,',
		'	Status,',
		'	HeadLinePart1,',
		'	HeadLinePart2,',
		'	Description,',
		'	PolicySummary,',
		'	Path1,',
		'	Path2',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND AdType = \'EXPANDED_TEXT_AD\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND LENGTH( Description ) <= 60',
	].join( '\n' ),
	AD_LAST_CHAR_IS_NOT_SPECIAL : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	AdGroupStatus,',
		'	Status,',
		'	HeadLinePart1,',
		'	HeadLinePart2,',
		'	Description,',
		'	PolicySummary,',
		'	Path1,',
		'	Path2',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND AdType = \'EXPANDED_TEXT_AD\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND NOT SUBSTR( Description, -1 ) IN unnest( [ \'?\', \'!\', \'.\' ] ) AND LENGTH( Description ) < 80',
	].join( '\n' ),
	CAMPAIGN_MISSING_MOBILE_BID_MODIFIER : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	CampaignStatus,',
		'	CampaignMobileBidModifier,',
		'	AdvertisingChannelType,',
		'	CAST( NULL AS STRING ) AS TargetingSetting,',
		'	CAST( NULL AS STRING ) AS DeliveryMethod,',
		'	CAST( NULL AS STRING ) AS AdRotationType,',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.CAMPAIGN_BASIC_STATS ).join( '.' ) + '` AS stat',
		'  ON stat.ExternalCustomerId = campaign.ExternalCustomerId',
		'  AND stat.CampaignId = campaign.CampaignId',
		'  AND DATE_DIFF( CURRENT_DATE( \'Europe/Berlin\' ), stat._DATA_DATE, DAY ) <= 30',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND CampaignMobileBidModifier IS NULL',
		'GROUP BY CustomerDescriptiveName, AccountDescriptiveName, campaign.CampaignId, customer.ExternalCustomerId, CampaignName, CampaignStatus, AdvertisingChannelType, CampaignMobileBidModifier ',
		'HAVING SUM( Clicks ) > 0',
	].join( '\n' ),
	CAMPAIGN_MULTI_CHANNEL : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	CampaignMobileBidModifier,',
		'	AdvertisingChannelType,',
		'	TargetingSetting,',
		'	DeliveryMethod,',
		'	AdRotationType',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as settings',
		'	ON  settings.ExternalCustomerId = campaign.ExternalCustomerId',
		'	AND settings.CampaignId = campaign.CampaignId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND AdvertisingChannelType = \'MULTI_CHANNEL\'',
		'	AND ifnull( settings._LATEST_DATE = settings._DATA_DATE , true )',
	].join( '\n' ),
	CAMPAIGN_TARGET_AND_BID : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	CampaignStatus,',
		'	CampaignMobileBidModifier,',
		'	AdvertisingChannelType,',
		'	TargetingSetting,',
		'	DeliveryMethod,',
		'	AdRotationType,',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as settings',
		'	ON  settings.ExternalCustomerId = campaign.ExternalCustomerId',
		'	AND settings.CampaignId = campaign.CampaignId',
		'	AND settings._LATEST_DATE = settings._DATA_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	-- targeting is narrowed',
		'	AND TargetingSetting = \'TARGET_ALL_FALSE\'',
		'	AND AdvertisingChannelType = \'SEARCH\'',
		'  AND ( FALSE',
		'	-- but ',
		'    OR REGEXP_CONTAINS( UPPER( CampaignName ), \'SUCH\' )',
		'    OR REGEXP_CONTAINS( UPPER( CampaignName ), \'SEARCH\' )',
		'    OR REGEXP_CONTAINS( UPPER( CampaignName ), \'SE-\' )',
		'    OR REGEXP_CONTAINS( UPPER( CampaignName ), \'GSN\' )',
		'    --OR REGEXP_CONTAINS( UPPER( CampaignName ), \'GDN\' )',
		'    --OR REGEXP_CONTAINS( UPPER( CampaignName ), \'DISPLAY\' )',
		'  )',
		'	--AND NOT ( FALSE',
		'	--	OR REGEXP_CONTAINS( UPPER( CampaignName ), \'RLSA\' )',
		'	--	OR REGEXP_CONTAINS( UPPER( CampaignName ), \'REMARKETING\' )',
		'	--	OR REGEXP_CONTAINS( UPPER( CampaignName ), \'RMK\' )',
		'	--	OR REGEXP_CONTAINS( UPPER( CampaignName ), \'WETTBEWERBER\' )',
		'	--	OR REGEXP_CONTAINS( UPPER( CampaignName ), \'NEUKUNDEN\' )',
		'	--)',
	].join( '\n' ),
	CAMPAIGN_NON_STANDARD_DELIVERY_METHOD : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'   CampaignMobileBidModifier,',
		'	AdvertisingChannelType,',
		'	TargetingSetting,',
		'	DeliveryMethod,',
		'	AdRotationType,',
		//'	STRING_AGG( SearchBudgetLostImpressionShare, \', \' ) AS SearchBudgetLostImpressionShares',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as settings',
		'	ON  settings.ExternalCustomerId = campaign.ExternalCustomerId',
		'	AND settings.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CAMPAIGN_CROSS_DEVICE_STATS ).join( '.' ) + '` as stat',
		'	ON  stat.ExternalCustomerId = campaign.ExternalCustomerId',
		'	AND stat.CampaignId = campaign.CampaignId',
		'	AND DATE_DIFF( CURRENT_DATE( \'Europe/Berlin\' ), stat._DATA_DATE, DAY ) <= 5',
		'	AND SearchBudgetLostImpressionShare != \'0.00%\'',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND settings._LATEST_DATE = settings._DATA_DATE',
		'	AND DeliveryMethod != \'STANDARD\'',
		'GROUP BY',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	CampaignMobileBidModifier ,',
		'	AdvertisingChannelType,',
		'	TargetingSetting,',
		'	DeliveryMethod,',
		'	AdRotationType',
	].join( '\n' ),
	CAMPAIGN_ROTATION_TYPE_NOT_OPTIMIZED : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	CampaignMobileBidModifier,',
		'	AdvertisingChannelType,',
		'	TargetingSetting,',
		'	DeliveryMethod,',
		'	AdRotationType,',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as settings',
		'	ON  settings.ExternalCustomerId = campaign.ExternalCustomerId',
		'	AND settings.CampaignId = campaign.CampaignId',
		'	AND settings._LATEST_DATE = settings._DATA_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND AdRotationType NOT IN ( \'OPTIMIZE\', \'CONVERSION_OPTIMIZE\' )',
	].join( '\n' ),
	CLOSE_VARIANTS : [ `WITH
sq AS (
	SELECT
		ExternalCustomerId,
		CampaignId,
		AdGroupId,
		CriterionId,
		Query,
	FROM ${ tsReference( 'SearchQueryStats'  ) }
	WHERE true
		AND _LATEST_DATE = _DATA_DATE
		--AND DATE_DIFF( CURRENT_DATE(), Date, DAY ) <= 30
	GROUP BY ExternalCustomerId, CampaignId, AdGroupId, CriterionId, Query
),
other_sq AS (
	SELECT
		sq2.ExternalCustomerId,
		sq2.CampaignId,
		sq2.AdGroupId,
		sq2.CriterionId,
		sq2.Query,
	FROM ${ tsReference( 'SearchQueryStats'  ) } as sq2
	JOIN ${ tsReference( 'Keyword'  ) } as other_keyword
		ON  other_keyword.ExternalCustomerId = sq2.ExternalCustomerId
		AND other_keyword.CampaignId         = sq2.CampaignId
		AND other_keyword.AdGroupId          = sq2.AdGroupId
		AND other_keyword.CriterionId        = sq2.CriterionId
	WHERE true
		AND sq2._LATEST_DATE = sq2._DATA_DATE
		--AND DATE_DIFF( CURRENT_DATE(), Date, DAY ) <= 30
		AND other_keyword._LATEST_DATE = other_keyword._DATA_DATE
		AND other_keyword.Status IN ( 'ENABLED', 'PAUSED' )
		AND other_keyword.KeywordMatchType = 'EXACT'
	GROUP BY sq2.ExternalCustomerId, CampaignId, AdGroupId, CriterionId, Query
),
result AS (
	SELECT
		ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
		CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
		CAST( campaign.CampaignId as STRING ) as CampaignId,
		CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
		CampaignName as Campaign_,
		AdGroupName as AdGroup_,
		keyword.Criteria as Keyword_,
		sq.Query,
		campaign.CampaignStatus,
		adgroup.AdGroupStatus,
		keyword.Status,
		count(*) as count
	FROM sq
	JOIN ${ tsReference( 'Customer'  ) } AS customer USING( ExternalCustomerId )
	JOIN ${ tsReference( 'Campaign'  ) } AS campaign USING( ExternalCustomerId, CampaignId )
	JOIN ${ tsReference( 'AdGroup'   ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
	JOIN ${ tsReference( 'Keyword'   ) } AS keyword  USING( ExternalCustomerId, CampaignId, AdGroupId, CriterionId )
	JOIN other_sq USING( ExternalCustomerId, Query )
	WHERE TRUE
		AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
		AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
		AND keyword.Status IN ( 'ENABLED', 'PAUSED' )
		AND keyword.KeywordMatchType = 'EXACT'
		AND customer._LATEST_DATE = customer._DATA_DATE
		AND campaign._LATEST_DATE = campaign._DATA_DATE
		AND adgroup._LATEST_DATE = adgroup._DATA_DATE
		AND keyword._LATEST_DATE = keyword._DATA_DATE
	GROUP BY
		customer.ExternalCustomerId,
		AccountName,
		CampaignName,
		AdGroupId,
		CampaignId,
		AdGroupName,
		keyword.Criteria,
		campaign.CampaignStatus,
		adgroup.AdGroupStatus,
		keyword.Status,
		Query
	HAVING count > 1
)
SELECT
	AccountName,
	ExternalCustomerId,
	Campaign_ AS Campaign,
	CampaignId,
	AdGroupId,
	AdGroup_ AS AdGroup,
	CampaignStatus,
	AdGroupStatus,
	Status,
	Keyword_ as exact_keyword,
	Query
FROM result
ORDER BY AccountName, query` ][ 0 ],
	DUPLICATE_CONVERSION_TRACKER : [
		'#StandardSQL',
		'SELECT',
		'	AccountName,',
		'	ExternalCustomerId,',
		'	STRING_AGG( CONCAT( ConversionTypeName, \'[ \', cast( ConversionTrackerId as STRING ), \' ]\' ) , \', \' ) as conversion_tracker_ids,',
		'	count(*) as count,',
		'	conversions,',
		'	value',
		'FROM (',
		'	SELECT',
		'		ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'		CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'		ConversionTrackerId,',
		'		ConversionTypeName,',
		'		sum( Conversions ) as conversions,',
		'		sum( ConversionValue ) as value',
		'	FROM `' + Object.values( BQ.CRITERIA_CONVERSION_STATS ).join( '.' ) + '` as k',
		'	JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = k.ExternalCustomerId',
		'	WHERE TRUE',
		'		AND DATE_DIFF( CURRENT_DATE(), k._DATA_DATE, DAY ) <= 30',
		'		AND customer._LATEST_DATE = customer._DATA_DATE',
		'		--AND k._LATEST_DATE = k._DATA_DATE',
		'	GROUP BY customer.ExternalCustomerId, AccountName, ConversionTrackerId, ConversionTypeName',
		'	ORDER BY AccountName, ConversionTrackerId, ConversionTypeName',
		')',
		'GROUP BY',
		'	ExternalCustomerId,',
		'	AccountName,',
		'	conversions,',
		'	value',
		'HAVING count > 1',
		'	AND conversions > 1',
	].join( '\n' ),
	EXCLUDED_GEO_LOCATIONS : [
	`SELECT
	ifnull( CustomerDescriptiveName,
	AccountDescriptiveName ) AS AccountName,
	CAST( criterion.ExternalCustomerId as STRING ) as ExternalCustomerId,
	campaign.CampaignName,
	campaign.CampaignStatus,
	CAST( criterion.CampaignId as STRING ) as CampaignId,
	CAST( criterion.CriterionId as STRING ) as CriterionId,
	--geo.Name,
	geo.TargetType,
	geo.CountryCode,
	geo.CanonicalName
FROM ${ tsReference( 'LocationBasedCampaignCriterion'  ) } AS criterion
JOIN ${ tsReference( 'p_Geotargets'  ) } AS geo
	ON TRUE
	AND criterion.CriterionId = geo.CriteriaId
JOIN ${ tsReference( 'Campaign'  ) } AS campaign
	ON TRUE
	AND criterion.ExternalCustomerId = campaign.ExternalCustomerId
	AND criterion.CampaignId = campaign.CampaignId
	AND campaign._DATA_DATE = campaign._LATEST_DATE
JOIN ${ tsReference( 'Customer'  ) } AS customer
ON TRUE
	AND customer.ExternalCustomerId = criterion.ExternalCustomerId
	AND customer._LATEST_DATE = customer._DATA_DATE
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND criterion._DATA_DATE = criterion._LATEST_DATE
	AND criterion.isNegative` ][ 0 ],
	EXCLUDED_CONTENT_LABELS : [
	`SELECT
	ifnull( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	campaign.CampaignName,
	CAST( settings.CampaignId as STRING ) as CampaignId,
	campaign.CampaignStatus,
	settings.ExcludedContentLabels
FROM ${ tsReference( 'CampaignSettings'  ) } AS settings
JOIN ${ tsReference( 'Campaign'  ) } AS campaign
	ON TRUE
	AND settings.ExternalCustomerId = campaign.ExternalCustomerId
	AND settings.CampaignId = campaign.CampaignId
	AND campaign._DATA_DATE = campaign._LATEST_DATE
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
JOIN ${ tsReference( 'Customer'  ) } AS customer
	ON TRUE
	AND customer.ExternalCustomerId = settings.ExternalCustomerId
	AND customer._LATEST_DATE = customer._DATA_DATE
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND settings._DATA_DATE = settings._LATEST_DATE
	AND ExcludedContentLabels is not null` ][ 0 ],
	EXTENSION_NO_SITE_LINKS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_site_links = 0',
	].join( '\n' ),
	EXTENSION_TOO_FEW_SITE_LINKS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_site_links in ( 1, 2, 3 )',
	].join( '\n' ),
	EXTENSION_NO_CALLOUTS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_callouts = 0',
	].join( '\n' ),
	EXTENSION_TOO_FEW_CALLOUTS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_callouts in ( 1, 2, 3 )',
	].join( '\n' ),
	EXTENSION_NO_SNIPPETS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_snippets = 0',
	].join( '\n' ),
	EXTENSION_TOO_FEW_SNIPPETS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_snippets in ( 1, 2, 3 )',
	].join( '\n' ),
	EXTENSION_NO_MESSAGES : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_messages = 0',
	].join( '\n' ),
	EXTENSION_NO_PHONE_NUMBERS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_phone_numbers = 0',
	].join( '\n' ),
	EXTENSION_NO_MOBILE_APPS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	ifnull( sum( cast( EntityType = \'sitelinks\' as int64 ) ), 0 ) as count_site_links,',
		'	ifnull( sum( cast( EntityType = \'callouts\' as int64 ) ), 0 ) as count_callouts,',
		'	ifnull( sum( cast( EntityType = \'snippets\' as int64 ) ), 0 ) as count_snippets,',
		'	ifnull( sum( cast( EntityType = \'messages\' as int64 ) ), 0 ) as count_messages,',
		'	ifnull( sum( cast( EntityType = \'phoneNumbers\' as int64 ) ), 0 ) as count_phone_numbers,',
		'	ifnull( sum( cast( EntityType = \'mobileApps\' as int64 ) ), 0 ) as count_mobile_apps',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CAMPAIGN_MAP ).join( '.' ) + '` AS map',
		'	ON campaign.ExternalCustomerId = map.ExternalCustomerId',
		'	AND campaign.CampaignId = map.CampaignId',
		'	AND map._DATA_DATE = map._LATEST_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'	AND customer._DATA_DATE = customer._LATEST_DATE',
		'	-- take into account only campaigns which are served by adwords-script',
		'	AND campaign.ExternalCustomerId IN (',
		'		SELECT ExternalCustomerId',
		'		FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as x ',
		'		WHERE x._DATA_DATE = x._LATEST_DATE',
		'		GROUP BY ExternalCustomerId',
		'	)',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignId',
		'HAVING TRUE',
		'	AND count_mobile_apps = 0',
	].join( '\n' ),
	INFORMATIONAL_SEARCH_QUERY : [
		'#StandardSQL',
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	Query',
		'FROM `' + Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) + '` as sq',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON sq.ExternalCustomerId = campaign.ExternalCustomerId AND sq.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON sq.ExternalCustomerId = adgroup.ExternalCustomerId AND sq.AdGroupId = adgroup.AdGroupId',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword ON sq.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.AdGroupId = sq.AdGroupId AND keyword.CriterionId = sq.CriterionId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND sq._LATEST_DATE = sq._DATA_DATE',
		'	--AND DATE_DIFF( CURRENT_DATE(), sq._DATA_DATE, DAY ) <= 30',
		'	AND REGEXP_CONTAINS( sq.Query, \'^was\\\\s|^wo\\\\s|^wie\\\\s|^wann\\\\s|^warum\\\\s|^wieso\\\\s|^weshalb\\\\s|^wer\\\\s|^wen\\\\s\' )',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	AccountName,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	Query,',
		'	AdGroupId,',
		'	CampaignId',
	].join( '\n' ),
	KEYWORD_TARGET_URL_MULTIPLE_QUESTION_MARKS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	KeywordMatchType,',
		'	FinalUrls',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND NOT IsNegative',
		'	AND REGEXP_CONTAINS( FinalUrls, \'\\\\?.*\\\\?\' )',
		'	AND NOT REGEXP_CONTAINS( FinalUrls, \',\' ) -- consider only lists with one url',
	].join( '\n' ),
	KEYWORD_TARGET_URL_MISSING : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	KeywordMatchType,',
		'	FinalUrls',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND AdvertisingChannelType = \'SEARCH\'',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND NOT IsNegative',
		'	AND FinalUrls is NULL',
	].join( '\n' ),
	KEYWORD_CAMPAIGN_MATCH_TYPE_MISMATCH : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	KeywordMatchType,',
		'	FinalUrls',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND NOT IsNegative ',
		'	AND (',
		'		( KeywordMatchType != \'BROAD\' AND REGEXP_CONTAINS( UPPER( CampaignName ), \'BROAD\' ) )',
		'		OR',
		'		( KeywordMatchType != \'EXACT\' AND REGEXP_CONTAINS( UPPER( CampaignName ), \'EXACT\' ) )',
		'		OR',
		'		( KeywordMatchType != \'BROAD\' AND REGEXP_CONTAINS( UPPER( CampaignName ), \'BMM\' ) )',
		'	)',
	].join( '\n' ),
	KEYWORD_ADGROUP_MATCH_TYPE_MISMATCH : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	KeywordMatchType,',
		'	FinalUrls',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND NOT IsNegative ',
		'	AND (',
		'		( KeywordMatchType != \'BROAD\' AND REGEXP_CONTAINS( UPPER( AdGroupName ), \'BROAD\' ) )',
		'		OR',
		'		( KeywordMatchType != \'EXACT\' AND REGEXP_CONTAINS( UPPER( AdGroupName ), \'EXACT\' ) )',
		'		OR',
		'		( KeywordMatchType != \'BROAD\' AND REGEXP_CONTAINS( UPPER( AdGroupName ), \'BMM\' ) )',
		'	)',
	].join( '\n' ),
	KEYWORD_SESSION_ID_IN_URL : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	KeywordMatchType,',
		'	FinalUrls',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND keyword.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND NOT IsNegative',
		'	AND REGEXP_CONTAINS( ifnull( FinalUrls, \'\' ), \'session[-_]?[Ii]d\' )',
	].join( '\n' ),
	KEYWORD_MODIFIED_NEGATIVE : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	Criteria,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	Status,',
		'	KeywordMatchType,',
		'	FinalUrls',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND keyword.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND keyword.IsNegative',
		'	AND REGEXP_CONTAINS( CONCAT( \' \', Criteria ), \'\\\\s\\\\+\' )',
	].join( '\n' ),
	LOCATION_BID_MODIFIER_MISSING : [
		'#StandardSQL',
		'SELECT',
		'	IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,',
		'	CAST( customer.ExternalCustomerId AS STRING ) AS ExternalCustomerId,',
		'	CAST( campaign.CampaignId AS STRING ) AS CampaignId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus,',
		'	COUNT(*) AS GeoLocationCount,',
		'	SUM( CAST( ( location.BidModifier IS NULL ) AS int64 ) ) AS GeoLocationWithoutBidModifierCount',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` AS campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer',
		'	ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.LOCATION_BASED_CAMPAIGN_CRITERION ).join( '.' ) + '` AS location',
		'	ON campaign.ExternalCustomerId = location.ExternalCustomerId',
		'	AND campaign.campaignId = location.CampaignId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND location._LATEST_DATE = location._DATA_DATE',
		'	AND NOT isNegative',
		'GROUP BY customer.ExternalCustomerId, AccountName, CampaignName, CampaignId, campaign.CampaignStatus',
		'HAVING TRUE',
		'	AND GeoLocationCount > 1',
		'	AND GeoLocationWithoutBidModifierCount > 0',
		'ORDER BY AccountName, CampaignName',
	].join( '\n' ),
	MULTIPLE_DOMAINS_IN_ADGROUP : [
		'#StandardSQL',
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	CampaignName,',
		'	AdGroupName,',
		'	STRING_AGG( DISTINCT REGEXP_Extract( keyword.FinalUrls, \'[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\\\\.[a-zA-Z]{2,}\\\\/\' ) , \', \' ) as domains,',
		'	count(*) as countKeywords,',
		'	count( DISTINCT REGEXP_Extract( keyword.FinalUrls, \'[a-zA-Z0-9][a-zA-Z0-9-]{1,61}[a-zA-Z0-9]\\\\.[a-zA-Z]{2,}\\\\/\' ) ) as countDomains',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'	JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND campaign.CampaignId = keyword.CampaignId',
		'	JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'	JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND adgroup.AdGroupId = keyword.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND NOT keyword.IsNegative',
		'	AND keyword.ApprovalStatus = \'APPROVED\'',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'group by customer.ExternalCustomerId, AccountName, CampaignName, AdGroupName, AdGroupId, CampaignId, campaign.CampaignStatus, adgroup.AdGroupStatus',
		'having countDomains > 1',
	].join( '\n' ),
	REPLACEABLE_NEGATIVE_KEYWORDS : [
		'#StandardSQL',
		'SELECT',
		'	AccountName,',
		'	ExternalCustomerId,',
		'	campaigns.CampaignId,',
		'	campaigns.CampaignStatus,',
		'	negKeywords.CampaignName,',
		'	Criteria as negative_keyword,',
		'	KeywordMatchType as match_type,',
		'	countAdgroups as count',
		'FROM (',
		'	SELECT',
		'		ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'		CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'		CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'		CampaignName,',
		'		Criteria,',
		'		keyword.KeywordMatchType',
		'		,count( DISTINCT AdGroupName ) as countAdgroups',
		'	FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'	JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND campaign.CampaignId = keyword.CampaignId',
		'	JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'	JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND adgroup.AdGroupId = keyword.AdGroupId',
		'	WHERE TRUE',
		'		AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'		AND keyword.IsNegative',
		'		AND keyword.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'		AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'		AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'		AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'		AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'		AND customer._LATEST_DATE = customer._DATA_DATE',
		'		AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	GROUP BY customer.ExternalCustomerId, AccountName, campaign.CampaignId, CampaignName, Criteria, KeywordMatchType',
		') AS negKeywords',
		'JOIN (',
		'	SELECT',
		'		CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'		CampaignStatus,',
		'		count( AdGroupName ) as countAdgroupsInCampaign',
		'	FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign',
		'	JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = campaign.ExternalCustomerId AND campaign.CampaignId = adgroup.CampaignId',
		'	WHERE TRUE',
		'		AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'		AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'		AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'		AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'		AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	GROUP BY CampaignId, CampaignName, CampaignStatus',
		') as campaigns',
		'ON negKeywords.CampaignId = campaigns.CampaignId',
		'WHERE countAdgroups = countAdgroupsInCampaign',
		'	AND countAdgroups > 1',
	].join( '\n' ),
	SEARCH_QUERIES_TRIGGERED_BY_MORE_THAN_ONE_KEYWORD : [
		'#StandardSQL',
		'SELECT',
		'	AccountName,',
		'	CAST( ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	Query,',
		'	CampaignName,',
		'	CampaignId,',
		'	AdGroupName,',
		'	AdGroupId,',
		'	CampaignStatus,',
		'	AdGroupStatus,',
		'	Status,',
		'	Criteria,',
		'	CriterionId,',
		'	KeywordMatchType,',
		'	Impressions,',
		'	Clicks,',
		'	Cost,',
		'	Conversions,',
		'	Cpo,',
		'	Cvr,',
		'	DENSE_RANK() OVER ( PARTITION BY ExternalCustomerId ORDER BY FARM_FINGERPRINT( CONCAT( AccountName, CampaignName, Query ) ) ) as Rank',
		'FROM (',
		'	SELECT',
		'		*',
		'		,',
		'		COUNT(*) OVER (',
		'			PARTITION BY',
		'			ExternalCustomerId,',
		'			AccountName,',
		'			CampaignId,',
		'			Query',
		'		) AS CriteriaCount',
		'	FROM (',
		'		SELECT',
		'			ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'			customer.ExternalCustomerId,',
		'			Query,',
		'			campaign.CampaignName,',
		'			campaign.CampaignId,',
		'			adgroup.AdGroupName,',
		'			adgroup.AdGroupId,',
		'			campaign.CampaignStatus,',
		'			adgroup.AdGroupStatus,',
		'			keyword.Status,',
		'			keyword.Criteria,',
		'			keyword.CriterionId,',
		'			keyword.KeywordMatchType,',
		'			SUM( Impressions ) AS Impressions,',
		'			SUM( Clicks ) AS Clicks,',
		'			SUM( Cost ) / 1000000 AS Cost,',
		'			SUM( Conversions ) AS Conversions,',
		'			ROUND( SUM( Cost ) / 1000000 / GREATEST( SUM( Conversions ), .01 ), 2 ) AS Cpo,',
		'			ROUND( SUM( Conversions ) / GREATEST( SUM( Clicks ), 1 ), 4 ) AS Cvr',
		'		FROM `' + Object.values( BQ.SEARCH_QUERY_STATS ).join( '.' ) + '` as sq',
		'		JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = sq.ExternalCustomerId AND sq.CampaignId = campaign.CampaignId',
		'		JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = sq.ExternalCustomerId',
		'		JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = sq.ExternalCustomerId AND sq.CampaignId = adgroup.CampaignId AND sq.AdGroupId = adgroup.AdGroupId',
		'		JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword ON keyword.ExternalCustomerId = sq.ExternalCustomerId AND sq.CampaignId = keyword.CampaignId AND sq.AdGroupId = keyword.AdGroupId AND keyword.CriterionId = sq.CriterionId',
		'		WHERE TRUE',
		'			AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'			AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'			AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'			AND keyword.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'			AND NOT keyword.IsNegative',
		'			AND customer._LATEST_DATE = customer._DATA_DATE',
		'			AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'			AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'			AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'			AND sq._LATEST_DATE = sq._DATA_DATE',
		'			--AND DATE_DIFF( sq._LATEST_DATE, sq._DATA_DATE, DAY ) <= 30',
		'		GROUP BY',
		'			customer.ExternalCustomerId,',
		'			AccountName,',
		'			Query,',
		'			campaign.CampaignName,',
		'			campaign.CampaignId,',
		'			adgroup.AdGroupName,',
		'			adgroup.AdGroupId,',
		'			keyword.Criteria,',
		'			keyword.CriterionId,',
		'			campaign.CampaignStatus,',
		'			adgroup.AdGroupStatus,',
		'			keyword.Status,',
		'			keyword.KeywordMatchType',
		'	)',
		')',
		'WHERE TRUE',
		'	AND CriteriaCount > 1',
		'ORDER BY',
		'	--CriteriaCount DESC,',
		'	ExternalCustomerId,',
		'	AccountName,',
		'	Rank',
	].join( '\n' ),
	WORLDWIDE_TARGETING : [
		'#StandardSQL',
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	campaign.CampaignName,',
		'	campaign.CampaignStatus',
		'FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.LOCATION_BASED_CAMPAIGN_CRITERION ).join( '.' ) + '` as location',
		'	ON campaign.ExternalCustomerId = location.ExternalCustomerId',
		'	AND campaign.campaignId = location.CampaignId',
		'	AND location._LATEST_DATE = location._DATA_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND NOT REGEXP_CONTAINS( LOWER( CampaignName ), \'welt|world|glo|alle\\\\sstädte\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND location.CriterionId is null',
		'ORDER BY AccountName, CampaignName',
	].join( '\n' ),
	DISAPPROVED_EXTENSIONS : [
		'#StandardSQL',
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	disapproved.DisapprovalShortNames as disapprovalReasons,',
		'	disapproved.PlaceholderType,',
		'	UPPER( disapproved.Status ) AS Status,',
		'	ifnull( ifnull( site_link.LinkText, ifnull( callout.Text, ifnull( messages.ExtensionText, ifnull( apps.LinkText, ifnull( cast( phones.Id as string), snippets.Values ) ) ) ) ), \'\' ) as text',
		'FROM `' + Object.values( BQ.EXTENSIONS_DISAPPROVED ).join( '.' ) + '` as disapproved',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer',
		'	ON customer.ExternalCustomerId = disapproved.ExternalCustomerId',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_SITELINKS ).join( '.' ) + '` as site_link',
		'	ON site_link.ExternalCustomerId = disapproved.ExternalCustomerId',
		'	AND site_link.id = disapproved.FeedItemId',
		'	AND site_link._LATEST_DATE = site_link._DATA_DATE',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_CALLOUTS ).join( '.' ) + '` as callout',
		'	ON callout.ExternalCustomerId = disapproved.ExternalCustomerId',
		'	AND callout.id = disapproved.FeedItemId',
		'	AND callout._LATEST_DATE = callout._DATA_DATE',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_MESSAGES ).join( '.' ) + '` as messages',
		'	ON messages.ExternalCustomerId = disapproved.ExternalCustomerId',
		'	AND messages.id = disapproved.FeedItemId',
		'	AND messages._LATEST_DATE = messages._DATA_DATE',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_MOBILE_APPS ).join( '.' ) + '` as apps',
		'	ON apps.ExternalCustomerId = disapproved.ExternalCustomerId',
		'	AND apps.id = disapproved.FeedItemId',
		'	AND apps._LATEST_DATE = apps._DATA_DATE',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_PHONE_NUMBERS ).join( '.' ) + '` as phones',
		'	ON phones.ExternalCustomerId = disapproved.ExternalCustomerId',
		'	AND phones.id = disapproved.FeedItemId',
		'	AND phones._LATEST_DATE = phones._DATA_DATE',
		'LEFT JOIN `' + Object.values( BQ.EXTENSIONS_SNIPPETS ).join( '.' ) + '` as snippets',
		'	ON snippets.ExternalCustomerId = disapproved.ExternalCustomerId',
		'	AND snippets.id = disapproved.FeedItemId',
		'	AND snippets._LATEST_DATE = snippets._DATA_DATE',
		'WHERE TRUE',
		'	AND disapproved._LATEST_DATE = disapproved._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND disapproved.Status = disapproved.Status',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	disapproved.DisapprovalShortNames,',
		'	disapproved.PlaceholderType,',
		'	disapproved.Status,',
		'	text',
	].join( '\n' ),
	DUPLICATE_KEYWORDS_BY_CASE : [
		'#StandardSQL',
		'SELECT',
		'	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	CAST( campaign.CampaignId as STRING ) as CampaignId,',
		'	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	keyword.Status,',
		'	KeywordMatchType,',
		'	LOWER( keyword.Criteria ) as keyword,',
		'	count( * ) as count,',
		'	STRING_AGG( keyword.Criteria, \', \' ) as keywords',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON keyword.ExternalCustomerId = campaign.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON keyword.ExternalCustomerId = adgroup.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer',
		'	ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND keyword.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND NOT IsNegative',
		'GROUP BY',
		'	customer.ExternalCustomerId,',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	CampaignName,',
		'	AdGroupName,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	Status,',
		'	KeywordMatchType,',
		'	AdGroupId,',
		'	CampaignId,',
		'	keyword',
		'HAVING count > 1',
	].join( '\n' ),
	DUPLICATE_KEYWORD_IN_CAMPAIGN : [ `#StandardSQL
SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( keyword.CampaignId as STRING ) as CampaignId,
	CampaignName,
	campaign.CampaignStatus,
	Criteria,
	KeywordMatchType,
	count(*) as count
FROM ${ tsReference( 'Keyword'  ) } AS keyword
JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup ON keyword.ExternalCustomerId = adgroup.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
JOIN ${ tsReference( 'Campaign'  ) } AS campaign
	ON keyword.ExternalCustomerId = campaign.ExternalCustomerId
	AND keyword.CampaignId = campaign.CampaignId
	AND campaign.AdvertisingChannelType = 'SEARCH'
JOIN ${ tsReference( 'Customer'  ) } AS customer ON customer.ExternalCustomerId = campaign.ExternalCustomerId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND NOT keyword.isNegative
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED' )
	AND keyword.Status IN ( 'ENABLED' )
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND keyword._LATEST_DATE = keyword._DATA_DATE
GROUP BY
	customer.ExternalCustomerId,
	AccountName,
	CampaignName,
	campaign.CampaignStatus,
	Criteria,
	KeywordMatchType,
	CampaignId
HAVING count > 1
ORDER BY count desc` ][ 0 ],
	MISSING_EXCLUDED_CONTENT_LABELS : [
		'SELECT *',
		'FROM (',
		'  SELECT',
		'    ifnull( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,',
		'    CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'    campaign.CampaignName,',
		'    CAST( settings.CampaignId as STRING ) as CampaignId,',
		'    campaign.CampaignStatus,',
		'    ( SELECT ARRAY_TO_STRING(',
		'      ARRAY(',
		'        SELECT x',
		'        FROM UNNEST( SPLIT( "ADULTISH, BELOW_THE_FOLD, DP, EMBEDDED_VIDEO, GAMES, JUVENILE, PROFANITY, TRAGEDY, VIDEO, SOCIAL_ISSUES", ", " ) ) as x',
		'        LEFT JOIN UNNEST( SPLIT( ExcludedContentLabels, ", " ) ) as y',
		'          ON x = y',
		'        WHERE y is null',
		'        ORDER BY x',
		'      ),',
		'      \', \'',
		'    ) ) AS MissingExcludedContentLabels,',
		'    settings.ExcludedContentLabels',
		'  FROM `' + Object.values( BQ.CAMPAIGN_SETTINGS ).join( '.' ) + '` as settings',
		'  JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign',
		'    ON TRUE',
		'    AND settings.ExternalCustomerId = campaign.ExternalCustomerId',
		'    AND settings.CampaignId = campaign.CampaignId',
		'    AND campaign._DATA_DATE = campaign._LATEST_DATE',
		'    AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'  JOIN',
		'    `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` AS customer',
		'    ON TRUE',
		'    AND customer.ExternalCustomerId = settings.ExternalCustomerId',
		'    AND customer._LATEST_DATE = customer._DATA_DATE',
		'  WHERE TRUE',
		'    AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'    AND settings._DATA_DATE = settings._LATEST_DATE',
		'    AND ExcludedContentLabels is not null',
		')',
		'WHERE TRUE',
		'  AND MissingExcludedContentLabels != \'\'',
	].join( '\n' ),
	NEGATIVE_KEYWORD_IN_AD : [
		'SELECT',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	ifnull( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,',
		'	CampaignName,',
		'	AdGroupName,',
		'	CampaignStatus,',
		'	AdGroupStatus,',
		'	Status,',
		'	neg.Text as negative_keyword,',
		'	neg.MatchType as negative_match_type,',
		'	ad.HeadLinePart1,',
		'	ad.HeadLinePart2,',
		'	ad.description,',
		'	CAST( ad.CampaignId as STRING ) as CampaignId,',
		'	CAST( ad.AdGroupId as STRING ) as AdGroupId',
		'FROM (',
		'	SELECT',
		'		ExternalCustomerId,',
		'		CampaignId,',
		'		Text,',
		'		MatchType',
		'	FROM `' + Object.values( BQ.CAMPAIGN_NEGATIVE_KEYWORDS ).join( '.' ) + '`',
		'	WHERE TRUE',
		'		AND _LATEST_DATE = _DATA_DATE',
		'		AND MatchType != \'EXACT\'',
		') as neg',
		'JOIN (',
		'	SELECT',
		'		ExternalCustomerId,',
		'		CampaignId,',
		'		AdGroupId,',
		'		HeadLinePart1,',
		'		HeadLinePart2,',
		'		description,',
		'		Status',
		'	FROM `' + Object.values( BQ.AD ).join( '.' ) + '`',
		'	WHERE TRUE',
		'		AND _DATA_DATE = _LATEST_DATE',
		'		AND Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	) as ad',
		'	ON ad.ExternalCustomerId = neg.ExternalCustomerId AND ad.CampaignId = neg.CampaignId',
		'JOIN (',
		'	SELECT',
		'		ExternalCustomerId,',
		'		CampaignId,',
		'		AdGroupName,',
		'		AdGroupId,',
		'		AdGroupStatus',
		'	FROM `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '`',
		'	WHERE TRUE',
		'		AND _DATA_DATE = _LATEST_DATE',
		'		AND AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	) as adgroup',
		'	ON ad.ExternalCustomerId = adgroup.ExternalCustomerId AND ad.CampaignId = adgroup.CampaignId AND ad.AdGroupId = adgroup.AdGroupId',
		'JOIN (',
		'	SELECT',
		'		ExternalCustomerId,',
		'		CampaignName,',
		'		CampaignId,',
		'		CampaignStatus',
		'	FROM `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '`',
		'	WHERE TRUE',
		'		AND ( EndDate IS NULL OR EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'		AND _DATA_DATE = _LATEST_DATE',
		'		AND CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	) as campaign',
		'	ON ad.ExternalCustomerId = campaign.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ',
		'	ON neg.ExternalCustomerId = customer.ExternalCustomerId',
		'    AND customer._LATEST_DATE = customer._DATA_DATE',
		'WHERE TRUE',
		'  AND (',
		'    FALSE',
		'    OR REGEXP_CONTAINS( CONCAT( \' \', ad.HeadLinePart1, \' \' ), CONCAT( \' \', REGEXP_REPLACE( neg.Text, \'["\\\\+\\\\-\\\\[\\\\]]\', \'\' ), \' \' ) )',
		'    OR REGEXP_CONTAINS( CONCAT( \' \', ad.HeadLinePart2, \' \' ), CONCAT( \' \', REGEXP_REPLACE( neg.Text, \'["\\\\+\\\\-\\\\[\\\\]]\', \'\' ), \' \' ) )',
		'    OR REGEXP_CONTAINS( CONCAT( \' \', ad.Description, \' \' ), CONCAT( \' \', REGEXP_REPLACE( neg.Text, \'["\\\\+\\\\-\\\\[\\\\]]\', \'\' ), \' \' ) )',
		')',
	].join( '\n' ),
	DKI_ERRORS : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,',
		'	CAST( ad.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	CAST( ad.CampaignId as STRING ) as CampaignId,',
		'	adgroup.AdGroupName,',
		'	CAST( ad.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	ad.Status,',
		'	ad.HeadlinePart1,',
		'	ad.HeadlinePart2,',
		'	ad.Description',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = ad.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = ad.ExternalCustomerId AND ad.CampaignId = adgroup.CampaignId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND (',
		'		FALSE',
		'		OR ( TRUE',
		'			AND REGEXP_CONTAINS( ad.HeadlinePart1, \'{\\\\w+\' )',
		'			AND NOT REGEXP_CONTAINS( ad.HeadlinePart1, \'{[kK]ey[wW]ord:.+}\' )',
		'		)OR ( TRUE',
		'			AND REGEXP_CONTAINS( ad.HeadlinePart2, \'{\\\\w+\' )',
		'			AND NOT REGEXP_CONTAINS( ad.HeadlinePart2, \'{[kK]ey[wW]ord:.+}\' )',
		'		)OR ( TRUE',
		'			AND REGEXP_CONTAINS( ad.Description, \'{\\\\w+\' )',
		'			AND NOT REGEXP_CONTAINS( ad.Description, \'{[kK]ey[wW]ord:.+}\' )',
		'		)',
		'	)',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND ad.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
	].join( '\n' ),
	WRONG_DKI : [
		'SELECT',
		'	ifnull( CustomerDescriptiveName, AccountDescriptiveName ) AS AccountName,',
		'	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,',
		'	campaign.CampaignName,',
		'	CAST( ad.CampaignId as STRING ) as CampaignId,',
		'	adgroup.AdGroupName,',
		'	CAST( ad.AdGroupId as STRING ) as AdGroupId,',
		'	campaign.CampaignStatus,',
		'	adgroup.AdGroupStatus,',
		'	ad.Status,',
		'	ad.HeadlinePart1,',
		'	ad.HeadlinePart2,',
		'	ad.Description',
		'FROM `' + Object.values( BQ.AD ).join( '.' ) + '` as ad',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign ON campaign.ExternalCustomerId = ad.ExternalCustomerId AND ad.CampaignId = campaign.CampaignId',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer ON customer.ExternalCustomerId = ad.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup ON adgroup.ExternalCustomerId = ad.ExternalCustomerId AND ad.CampaignId = adgroup.CampaignId AND ad.AdGroupId = adgroup.AdGroupId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND (',
		'		FALSE',
		'		OR ( REGEXP_CONTAINS( ad.HeadlinePart1, \'{keyWord:.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.HeadlinePart1, \'{Keyword:.+\\\\s.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.HeadlinePart1, \'{keyword:.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.HeadlinePart2, \'{keyWord:.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.HeadlinePart2, \'{Keyword:.+\\\\s.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.HeadlinePart2, \'{keyword:.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.Description, \'{keyWord:.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.Description, \'{Keyword:.+\\\\s.+}\' )  )',
		'		OR ( REGEXP_CONTAINS( ad.Description, \'{keyword:.+}\' )  )',
		'	)',
		'	AND ad._LATEST_DATE = ad._DATA_DATE',
		'	AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'	AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'	AND ad.Status IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\', \'PAUSED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\', \'PAUSED\' )',
	].join( '\n' ),
	LOW_VOLUME_EXACT_EXCLUDED_ADGROUP_LEVEL : [
		'SELECT',
		'	Customer.AccountDescriptiveName,',
		'	Customer.ExternalCustomerId,',
		'	Campaign.CampaignName as CampaignName,',
		'	Campaign.CampaignId as CampaignId,',
		'	BroadAdGroup.AdGroupName as broadAdGroupName,',
		'	BroadAdGroup.AdGroupId as broadAdGroupId,',
		'	ExactNegativeKeywordInBroad.Criteria as exactNegativeKeyword',
		'FROM `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as Customer',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as Campaign',
		'	ON TRUE',
		'	AND Campaign.ExternalCustomerId = Customer.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as BroadAdGroup',
		'	ON TRUE',
		'	AND Campaign.ExternalCustomerId = BroadAdGroup.ExternalCustomerId',
		'	AND Campaign.CampaignId = BroadAdGroup.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as ExactAdGroup',
		'	ON TRUE',
		'	AND Campaign.ExternalCustomerId = ExactAdGroup.ExternalCustomerId',
		'	AND Campaign.CampaignId = ExactAdGroup.CampaignId',
		'	AND REGEXP_EXTRACT( LOWER( ExactAdGroup.AdGroupName ), \'(.*)(?:exact|exm)\' ) = REGEXP_EXTRACT( LOWER( BroadAdGroup.AdGroupName ), \'(.*)(?:broad|bmm)\' )',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as ExactKeyword',
		'	ON TRUE',
		'	AND Campaign.ExternalCustomerId = ExactKeyword.ExternalCustomerId',
		'	AND Campaign.CampaignId = ExactKeyword.CampaignId',
		'	AND ExactAdGroup.AdGroupId = ExactKeyword.AdGroupId',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as ExactNegativeKeywordInBroad',
		'	ON TRUE',
		'	AND ( Campaign.EndDate IS NULL OR Campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND Campaign.ExternalCustomerId = ExactNegativeKeywordInBroad.ExternalCustomerId',
		'	AND Campaign.CampaignId = ExactNegativeKeywordInBroad.CampaignId',
		'	AND BroadAdGroup.AdGroupId = ExactNegativeKeywordInBroad.AdGroupId',
		'	AND LOWER( ExactNegativeKeywordInBroad.Criteria ) = LOWER( ExactKeyword.Criteria )',
		'WHERE TRUE',
		'	AND Customer._LATEST_DATE = Customer._DATA_DATE',
		'	AND Campaign.CampaignStatus = \'ENABLED\'',
		'	AND Campaign._LATEST_DATE = Campaign._DATA_DATE',
		'	AND ExactAdGroup.AdGroupStatus = \'ENABLED\'',
		'	AND BroadAdGroup.AdGroupStatus = \'ENABLED\'',
		'	AND ExactAdGroup._LATEST_DATE = ExactAdGroup._DATA_DATE',
		'	AND BroadAdGroup._LATEST_DATE = BroadAdGroup._DATA_DATE',
		'	AND ExactKeyword.Status = \'ENABLED\'',
		'	AND ExactNegativeKeywordInBroad.Status = \'ENABLED\'',
		'	AND ExactKeyword._LATEST_DATE = ExactKeyword._DATA_DATE',
		'	AND ExactNegativeKeywordInBroad._LATEST_DATE = ExactNegativeKeywordInBroad._DATA_DATE',
		'	AND ExactKeyword.SystemServingStatus = \'RARELY_SERVED\'',
		'	AND ExactKeyword.KeywordMatchType = \'EXACT\'',
		'	AND ExactNegativeKeywordInBroad.KeywordMatchType = \'EXACT\'',
		'	AND ExactKeyword.IsNegative = false',
		'	AND ExactNegativeKeywordInBroad.IsNegative = true',
	].join( '\n' ),
	LOW_VOLUME_EXACT_EXCLUDED_CAMPAIGN_LEVEL : [
		'SELECT',
		'	Customer.AccountDescriptiveName,',
		'	Customer.ExternalCustomerId,',
		'	BroadCampaign.CampaignName as broadCampaignName,',
		'	BroadCampaign.CampaignId as broadCampaignId,',
		'	BroadAdGroup.AdGroupName as broadAdGroupName,',
		'	BroadAdGroup.AdGroupId as broadAdGroupId,',
		'	ExactNegativeKeywordInBroad.Criteria as exactNegativeKeyword',
		'FROM `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as Customer',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as ExactCampaign',
		'	ON TRUE',
		'	AND ExactCampaign.ExternalCustomerId = Customer.ExternalCustomerId',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as BroadCampaign',
		'	ON TRUE',
		'	AND ExactCampaign.ExternalCustomerId = BroadCampaign.ExternalCustomerId',
		'	AND REGEXP_EXTRACT( LOWER( ExactCampaign.CampaignName ), \'(.*)(?:exact|exm)\' ) = REGEXP_EXTRACT( LOWER( BroadCampaign.CampaignName ), \'(.*)(?:broad|bmm)\' )',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as BroadAdGroup',
		'	ON TRUE',
		'	AND BroadCampaign.ExternalCustomerId = BroadAdGroup.ExternalCustomerId',
		'	AND BroadCampaign.CampaignId = BroadAdGroup.CampaignId',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as ExactAdGroup',
		'	ON TRUE',
		'	AND ExactCampaign.ExternalCustomerId = ExactAdGroup.ExternalCustomerId',
		'	AND ExactCampaign.CampaignId = ExactAdGroup.CampaignId',
		'	AND BroadAdGroup.AdGroupName = ExactAdGroup.AdGroupName',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as ExactKeyword',
		'	ON TRUE',
		'	AND ExactCampaign.ExternalCustomerId = ExactKeyword.ExternalCustomerId',
		'	AND ExactCampaign.CampaignId = ExactKeyword.CampaignId',
		'	AND ExactAdGroup.AdGroupId = ExactKeyword.AdGroupId',
		'JOIN `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as ExactNegativeKeywordInBroad',
		'	ON TRUE',
		'	AND BroadCampaign.ExternalCustomerId = ExactNegativeKeywordInBroad.ExternalCustomerId',
		'	AND BroadCampaign.CampaignId = ExactNegativeKeywordInBroad.CampaignId',
		'	AND BroadAdGroup.AdGroupId = ExactNegativeKeywordInBroad.AdGroupId',
		'	AND LOWER( ExactNegativeKeywordInBroad.Criteria ) = LOWER( ExactKeyword.Criteria )',
		'WHERE TRUE',
		'	AND ( BroadCampaign.EndDate IS NULL OR BroadCampaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND ( ExactCampaign.EndDate IS NULL OR ExactCampaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND Customer._LATEST_DATE = Customer._DATA_DATE',
		'	AND ExactCampaign.CampaignStatus = \'ENABLED\'',
		'	AND BroadCampaign.CampaignStatus = \'ENABLED\'',
		'	AND ExactCampaign._LATEST_DATE = ExactCampaign._DATA_DATE',
		'	AND BroadCampaign._LATEST_DATE = BroadCampaign._DATA_DATE',
		'	AND ExactAdGroup.AdGroupStatus = \'ENABLED\'',
		'	AND BroadAdGroup.AdGroupStatus = \'ENABLED\'',
		'	AND ExactAdGroup._LATEST_DATE = ExactAdGroup._DATA_DATE',
		'	AND BroadAdGroup._LATEST_DATE = BroadAdGroup._DATA_DATE',
		'	AND ExactKeyword.Status = \'ENABLED\'',
		'	AND ExactNegativeKeywordInBroad.Status = \'ENABLED\'',
		'	AND ExactKeyword._LATEST_DATE = ExactKeyword._DATA_DATE',
		'	AND ExactNegativeKeywordInBroad._LATEST_DATE = ExactNegativeKeywordInBroad._DATA_DATE',
		'	AND ExactKeyword.SystemServingStatus = \'RARELY_SERVED\'',
		'	AND ExactKeyword.KeywordMatchType = \'EXACT\'',
		'	AND ExactNegativeKeywordInBroad.KeywordMatchType = \'EXACT\'',
		'	AND ExactKeyword.IsNegative = false',
		'	AND ExactNegativeKeywordInBroad.IsNegative = true',
	].join( '\n' ),
	NO_TRAFFIC_KEYWORDS : [
		'SELECT',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	customer.ExternalCustomerId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	campaign.CampaignId,',
		'	AdGroupName,',
		'	adgroup.AdGroupStatus,',
		'	adgroup.AdGroupId,',
		'	keyword.Criteria,',
		'	keyword.Status,',
		'  keyword.CriterionId,',
		'	KeywordMatchType,',
		'  SystemServingStatus,',
		'  SUM( Impressions ) as Impressions',
		'FROM `' + Object.values( BQ.KEYWORD ).join( '.' ) + '` as keyword',
		'JOIN `' + Object.values( BQ.CAMPAIGN ).join( '.' ) + '` as campaign',
		'  ON keyword.ExternalCustomerId = campaign.ExternalCustomerId',
		'  AND keyword.CampaignId = campaign.CampaignId',
		'  AND campaign._LATEST_DATE = campaign._DATA_DATE',
		'JOIN `' + Object.values( BQ.AD_GROUP ).join( '.' ) + '` as adgroup',
		'  ON keyword.ExternalCustomerId = adgroup.ExternalCustomerId',
		'  AND keyword.CampaignId = adgroup.CampaignId',
		'  AND keyword.AdGroupId = adgroup.AdGroupId',
		'  AND adgroup._LATEST_DATE = adgroup._DATA_DATE',
		'JOIN `' + Object.values( BQ.CUSTOMER ).join( '.' ) + '` as customer',
		'	ON customer.ExternalCustomerId = keyword.ExternalCustomerId',
		'	AND customer._LATEST_DATE = customer._DATA_DATE',
		'LEFT JOIN `' + Object.values( BQ.KEYWORD_BASIC_STATS ).join( '.' ) + '` AS stat',
		'	ON TRUE',
		'	AND DATE_DIFF( CURRENT_DATE( \'Europe/Berlin\' ), stat._DATA_DATE, DAY ) < 180',
		'	AND stat.ExternalCustomerId = customer.ExternalCustomerId',
		'	AND stat.CampaignId = campaign.CampaignId',
		'	AND stat.AdGroupId = adgroup.AdGroupId',
		'	AND stat.CriterionId = keyword.CriterionId',
		'WHERE TRUE',
		'	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( \'Europe/Berlin\' ) )',
		'	AND keyword._LATEST_DATE = keyword._DATA_DATE',
		'	AND campaign.CampaignStatus IN ( \'ENABLED\' )',
		'	AND adgroup.AdGroupStatus IN ( \'ENABLED\' )',
		'	AND keyword.Status IN ( \'ENABLED\' )',
		'	AND NOT IsNegative',
		'GROUP BY',
		'	CustomerDescriptiveName,',
		'	AccountDescriptiveName,',
		'	customer.ExternalCustomerId,',
		'	CampaignName,',
		'	campaign.CampaignStatus,',
		'	campaign.CampaignId,',
		'	AdGroupName,',
		'	adgroup.AdGroupStatus,',
		'	adgroup.AdGroupId,',
		'	keyword.Criteria,',
		'	keyword.Status,',
		'  keyword.CriterionId,',
		'	KeywordMatchType,',
		'  SystemServingStatus',
		'HAVING TRUE',
		'  AND Impressions IS NULL',
		'  AND SystemServingStatus = \'ELIGIBLE\'',
	].join( '\n' ),
	KEYWORD_CONTAINS_CAPITAL  : [ `SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	keyword.Status,
	KeywordMatchType,
	FinalUrls
FROM ${ tsReference( 'Keyword'  ) } as keyword
JOIN ${ tsReference( 'Customer' ) } as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId
JOIN ${ tsReference( 'Campaign' ) } as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'AdGroup'  ) } as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND Status IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
	AND keyword._LATEST_DATE = keyword._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND NOT IsNegative
	AND REGEXP_CONTAINS( Criteria, '[A-Z]' )` ][ 0 ],
	KEYWORD_CONTAINS_DASH     : [ `SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	keyword.Status,
	KeywordMatchType,
	FinalUrls
FROM ${ tsReference( 'Keyword'  ) } as keyword
JOIN ${ tsReference( 'Customer' ) } as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId
JOIN ${ tsReference( 'Campaign' ) } as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'AdGroup'  ) } as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND Status IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
	AND keyword._LATEST_DATE = keyword._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND NOT IsNegative
	AND REGEXP_CONTAINS( Criteria, '-' )` ][ 0 ],
	KEYWORD_CONTAINS_DOT      : [ `SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	keyword.Status,
	KeywordMatchType,
	FinalUrls
FROM ${ tsReference( 'Keyword'  ) } as keyword
JOIN ${ tsReference( 'Customer' ) } as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId
JOIN ${ tsReference( 'Campaign' ) } as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'AdGroup'  ) } as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND Status IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
	AND keyword._LATEST_DATE = keyword._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND NOT IsNegative
	AND REGEXP_CONTAINS( Criteria, r'\\.' )` ][ 0 ],
	KEYWORD_CONTAINS_QUOTES   : [ `SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	keyword.Status,
	KeywordMatchType,
	FinalUrls
FROM ${ tsReference( 'Keyword'  ) } as keyword
JOIN ${ tsReference( 'Customer' ) } as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId
JOIN ${ tsReference( 'Campaign' ) } as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'AdGroup'  ) } as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND Status IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
	AND keyword._LATEST_DATE = keyword._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND NOT IsNegative
	AND REGEXP_CONTAINS( Criteria, r'"' )` ][ 0 ],
	KEYWORD_CONTAINS_BRACKETS : [ `SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( adgroup.AdGroupId as STRING ) as AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	keyword.Status,
	KeywordMatchType,
	FinalUrls
FROM ${ tsReference( 'Keyword'  ) } as keyword
JOIN ${ tsReference( 'Customer' ) } as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId
JOIN ${ tsReference( 'Campaign' ) } as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'AdGroup'  ) } as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND Status IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
	AND keyword._LATEST_DATE = keyword._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND NOT IsNegative
	AND REGEXP_CONTAINS( Criteria, r'\\[' )` ][ 0 ],
	KEYWORD_BROAD_MATCH_STILL_ACTIVE : [ `
	SELECT
		IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
		CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
		CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
		CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
		CampaignName,
		AdGroupName,
		Criteria,
		CampaignStatus,
		AdGroupStatus,
		Status,
		KeywordMatchType,
		FinalUrls,
	FROM ${ tsReference( 'Keyword'  ) } as keyword
	JOIN ${ tsReference( 'Customer' ) } as customer ON customer.ExternalCustomerId = keyword.ExternalCustomerId
	JOIN ${ tsReference( 'Campaign' ) } as campaign ON campaign.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = campaign.CampaignId
	JOIN ${ tsReference( 'AdGroup'  ) } as adgroup ON adgroup.ExternalCustomerId = keyword.ExternalCustomerId AND keyword.CampaignId = adgroup.CampaignId AND keyword.AdGroupId = adgroup.AdGroupId
	WHERE TRUE
		AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
		AND Status IN ( 'ENABLED', 'PAUSED' )
		AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
		AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
		AND keyword. _LATEST_DATE = keyword. _DATA_DATE
		AND customer._LATEST_DATE = customer._DATA_DATE
		AND campaign._LATEST_DATE = campaign._DATA_DATE
		AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
		AND NOT IsNegative
		AND KeywordMatchType = 'BROAD'
		AND NOT REGEXP_CONTAINS( Criteria, r'^\\+' )
	` ][ 0 ],
	
	KEYWORD_BMM_STILL_ACTIVE : [ `-- keyword_bmm_still_active
	SELECT
		IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
		CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
		CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
		CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
		CampaignName,
		AdGroupName,
		Criteria,
		CampaignStatus,
		AdGroupStatus,
		Status,
		KeywordMatchType,
		FinalUrls,
	FROM ${ tsReference( 'Keyword'  ) } AS keyword
	JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
	JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
	JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
	WHERE TRUE
		AND Status                  IN ( 'ENABLED' )
		AND campaign.CampaignStatus IN ( 'ENABLED' )
		AND adgroup.AdGroupStatus   IN ( 'ENABLED' )
		AND customer._LATEST_DATE = customer._DATA_DATE
		AND campaign._LATEST_DATE = campaign._DATA_DATE
		AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
		AND keyword. _LATEST_DATE = keyword. _DATA_DATE
		AND NOT IsNegative
		AND KeywordMatchType = 'BROAD'
		AND REGEXP_CONTAINS( Criteria, r'^\+|\s\+' )
` ][ 0 ],

	KEYWORD_CONTAINS_PLUS : [ `-- keyword_contains_plus
SELECT
	IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
	CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
	CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
	CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	CampaignStatus,
	AdGroupStatus,
	Status,
	KeywordMatchType,
	FinalUrls,
FROM ${ tsReference( 'Keyword'  ) } AS keyword
JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
WHERE TRUE
	AND Status                  IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND keyword. _LATEST_DATE = keyword. _DATA_DATE
	AND NOT IsNegative
	AND REGEXP_CONTAINS( Criteria, r'\\+' )
` ][ 0 ],

	KEYWORD_BAD_QS : [ `-- keyword_bad_qs
SELECT
	IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
	CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
	CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
	CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	CampaignStatus,
	AdGroupStatus,
	Status,
	KeywordMatchType,
	FinalUrls,
FROM ${ tsReference( 'Keyword'  ) } AS keyword
JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
WHERE TRUE
	AND Status                  IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND keyword. _LATEST_DATE = keyword. _DATA_DATE
	AND NOT IsNegative
` ][ 0 ],
	
	KEYWORD_BELOW_AVERAGE_AD_RELEVANCE : [ `-- keyword_below_average_ad_relevance
SELECT
	IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
	CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
	CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
	CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	CampaignStatus,
	AdGroupStatus,
	Status,
	KeywordMatchType,
	FinalUrls,
FROM ${ tsReference( 'Keyword'  ) } AS keyword
JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
WHERE TRUE
	AND Status                  IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND keyword. _LATEST_DATE = keyword. _DATA_DATE
	AND NOT IsNegative
	AND CreativeQualityScore = 'BELOW_AVERAGE'
` ][ 0 ],
	
	KEYWORD_BELOW_AVERAGE_EXPECTED_CTR : [ `-- keyword_below_average_expected_ctr
SELECT
	IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
	CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
	CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
	CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	CampaignStatus,
	AdGroupStatus,
	Status,
	KeywordMatchType,
	FinalUrls,
FROM ${ tsReference( 'Keyword'  ) } AS keyword
JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
WHERE TRUE
	AND Status                  IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND keyword. _LATEST_DATE = keyword. _DATA_DATE
	AND NOT IsNegative
	AND SearchPredictedCtr = 'BELOW_AVERAGE'
` ][ 0 ],
	
	KEYWORD_BELOW_AVERAGE_LP_EXPERIANCE : [ `-- keyword_below_average_lp_experiance
SELECT
	IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) AS AccountName,
	CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
	CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
	CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
	CampaignName,
	AdGroupName,
	Criteria,
	CampaignStatus,
	AdGroupStatus,
	Status,
	KeywordMatchType,
	FinalUrls,
FROM ${ tsReference( 'Keyword'  ) } AS keyword
JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
WHERE TRUE
	AND Status                  IN ( 'ENABLED', 'PAUSED' )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND keyword. _LATEST_DATE = keyword. _DATA_DATE
	AND NOT IsNegative
	AND PostClickQualityScore = 'BELOW_AVERAGE'
` ][ 0 ],
	
	ADGROUP_TOO_MANY_KEYWORDS : [ `-- ad_group_too_many_keywords
SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( AdGroupId as STRING ) as AdGroupId,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	CampaignName,
	AdGroupName,
FROM ${ tsReference( 'AdGroup'   ) } AS adgroup
JOIN ${ tsReference( 'Campaign'  ) } AS campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'Customer'  ) } AS customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus IN ( 'ENABLED', 'PAUSED' )
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND adgroup._LATEST_DATE = adgroup._DATA_DATE
	AND (
    SELECT
      count(*)
	FROM ${ tsReference( 'Keyword'  ) } AS keyword
    WHERE TRUE
      AND keyword.ExternalCustomerId = adgroup.ExternalCustomerId 
      AND keyword.AdGroupId          = adgroup.AdGroupId
      AND NOT IsNegative
      AND keyword._LATEST_DATE = keyword._DATA_DATE
   ) > 20
	AND AdvertisingChannelType = 'SEARCH'
ORDER BY AccountName, CampaignName, AdGroupName
` ][ 0 ],
	
	CAMPAIGN_NON_BRAND_NO_SMART_BIDDING : [ `-- campaign_non_brand_no_smart_bidding
SELECT
	ANY_VALUE( IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) ) AS AccountName,
	ANY_VALUE( CAST( ExternalCustomerId  AS STRING ) ) AS ExternalCustomerId,
	ANY_VALUE( CAST( CampaignId          AS STRING ) ) AS CampaignId,
	ANY_VALUE( CampaignName              ) AS CampaignName,
	ANY_VALUE( CampaignStatus            ) AS CampaignStatus,
	ANY_VALUE( CampaignMobileBidModifier ) AS CampaignMobileBidModifier,
	ANY_VALUE( AdvertisingChannelType    ) AS AdvertisingChannelType,
	CAST( NULL AS STRING ) AS TargetingSetting,
	CAST( NULL AS STRING ) AS DeliveryMethod,
	CAST( NULL AS STRING ) AS AdRotationType,
  --STRING_AGG( DISTINCT BiddingStrategyType )     AS BiddingStrategyType,
  --ROUND( SUM( Conversions ), 2 )                 AS Conversions,
FROM ${ tsReference( 'Campaign'  ) } campaign
JOIN ${ tsReference( 'Customer'  ) } customer
  USING( ExternalCustomerId )
JOIN ${ tsReference( 'CampaignBasicStats'  ) } stat
  USING( ExternalCustomerId, CampaignId )
WHERE TRUE
  AND customer._DATA_DATE = customer._LATEST_DATE
  AND campaign._DATA_DATE = campaign._LATEST_DATE
  AND DATE_DIFF( CURRENT_DATE(), stat._DATA_DATE, DAY ) <= 30
  AND NOT REGEXP_CONTAINS( UPPER( CampaignName ), 'BRD' )
  AND NOT REGEXP_CONTAINS( UPPER( CampaignName ), 'BRAND' )
  --'Target Outranking Share', 'Target search page location'
  --AND BiddingStrategyType NOT IN ( 'cpc', 'cpm', 'cpv', 'Enhanced CPC', 'Maximize clicks' )
  -- not smart-bidding
  AND BiddingStrategyType NOT IN ( 'Target CPA', 'Target ROAS', 'Maximize Conversions',  'Maximize Conversion Value' )
GROUP BY
  campaign.ExternalCustomerId,
  campaign.CampaignId
HAVING
  SUM( Conversions ) > 30
` ][ 0 ],
	
	ADGROUP_TOO_MANY_RSA : [ `-- adgroup_too_many_rsa
SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( AdGroupId as STRING ) as AdGroupId,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	CampaignName,
	AdGroupName,
FROM ${ tsReference( 'AdGroup'   ) } AS adgroup
JOIN ${ tsReference( 'Campaign'  ) } AS campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'Customer'  ) } AS customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND (
    SELECT count(*) 
    FROM ${ tsReference( 'Ad'  ) } AS ad
    WHERE TRUE
      AND ad.ExternalCustomerId = adgroup.ExternalCustomerId
      AND ad.AdGroupId          = adgroup.AdGroupId
      AND AdType IN ( 'RESPONSIVE_SEARCH_AD', 'MULTI_ASSET_RESPONSIVE_DISPLAY_AD', 'RESPONSIVE_DISPLAY_AD' )
      AND ad._LATEST_DATE = ad._DATA_DATE
  ) > 2
	AND AdGroupType IN ( 'SEARCH_STANDARD', 'DISPLAY_STANDARD' )
ORDER BY AccountName, CampaignName, AdGroupName
` ][ 0 ],

	ADGROUP_NO_RSA : [ `-- adgroup_no_rsa
SELECT
	ifnull( CustomerDescriptiveName,  AccountDescriptiveName ) as AccountName,
	CAST( customer.ExternalCustomerId as STRING ) as ExternalCustomerId,
	CAST( campaign.CampaignId as STRING ) as CampaignId,
	CAST( AdGroupId as STRING ) as AdGroupId,
	campaign.CampaignStatus,
	adgroup.AdGroupStatus,
	CampaignName,
	AdGroupName,
FROM ${ tsReference( 'AdGroup'   ) } AS adgroup
JOIN ${ tsReference( 'Campaign'  ) } AS campaign ON campaign.ExternalCustomerId = adgroup.ExternalCustomerId AND adgroup.CampaignId = campaign.CampaignId
JOIN ${ tsReference( 'Customer'  ) } AS customer ON campaign.ExternalCustomerId = customer.ExternalCustomerId
WHERE TRUE
	AND ( campaign.EndDate IS NULL OR campaign.EndDate >= CURRENT_DATE( 'Europe/Berlin' ) )
	AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
	AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
	AND campaign._LATEST_DATE = campaign._DATA_DATE
	AND customer._LATEST_DATE = customer._DATA_DATE
	AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
	AND (
    SELECT count(*) 
    FROM ${ tsReference( 'Ad'  ) } AS ad
    WHERE TRUE
      AND ad.ExternalCustomerId = adgroup.ExternalCustomerId
      AND ad.AdGroupId          = adgroup.AdGroupId
      AND AdType IN ( 'RESPONSIVE_SEARCH_AD', 'MULTI_ASSET_RESPONSIVE_DISPLAY_AD', 'RESPONSIVE_DISPLAY_AD' )
      AND ad._LATEST_DATE = ad._DATA_DATE
  ) = 0
	AND AdGroupType IN ( 'SEARCH_STANDARD', 'DISPLAY_STANDARD' )
ORDER BY AccountName, CampaignName, AdGroupName
` ][ 0 ],

	ADGROUP_MULTIPLE_POOR_RELEVANCE_KEYWORDS : [ `
	-- adgroup_multiple_poor_relevance_keywords
		SELECT
			ANY_VALUE( IFNULL( CustomerDescriptiveName,  AccountDescriptiveName ) ) AS AccountName,
			CAST( customer.ExternalCustomerId AS STRING )              AS ExternalCustomerId,
			CAST( campaign.CampaignId         AS STRING )              AS CampaignId,
			CAST( adgroup.AdGroupId           AS STRING )              AS AdGroupId,
			ANY_VALUE( CampaignStatus ) AS CampaignStatus,
			ANY_VALUE( AdGroupStatus  ) AS AdGroupStatus,
			ANY_VALUE( CampaignName   ) AS CampaignName,
			ANY_VALUE( AdGroupName    ) AS AdGroupName,
		FROM ${ tsReference( 'Keyword'  ) } AS keyword
		JOIN ${ tsReference( 'Customer' ) } AS customer USING( ExternalCustomerId )
		JOIN ${ tsReference( 'Campaign' ) } AS campaign USING( ExternalCustomerId, CampaignId )
		JOIN ${ tsReference( 'AdGroup'  ) } AS adgroup  USING( ExternalCustomerId, CampaignId, AdGroupId )
		WHERE TRUE
			AND Status                  IN ( 'ENABLED', 'PAUSED' )
			AND campaign.CampaignStatus IN ( 'ENABLED', 'PAUSED' )
			AND adgroup.AdGroupStatus   IN ( 'ENABLED', 'PAUSED' )
			AND customer._LATEST_DATE = customer._DATA_DATE
			AND campaign._LATEST_DATE = campaign._DATA_DATE
			AND adgroup. _LATEST_DATE = adgroup. _DATA_DATE
			AND keyword. _LATEST_DATE = keyword. _DATA_DATE
			AND NOT IsNegative
			AND CreativeQualityScore = 'BELOW_AVERAGE'
		GROUP BY
			ExternalCustomerId,
			CampaignId,
			AdGroupId
		HAVING TRUE
		  AND COUNT(*) > 1
		ORDER BY
		  COUNT(*) DESC
	` ][ 0 ],

	AD : [ `-- ad
SELECT
  'duplicate_special_chars_in_description' AS Problem,
  *
FROM ${ pitfallReference( 'ad_duplicate_special_chars_in_description' ) }
UNION ALL
SELECT
  'last_char_is_not_special' AS Problem,
  *
FROM ${ pitfallReference( 'ad_last_char_is_not_special' ) } 
UNION ALL
SELECT
  'path1_missing_in_non_brand_campaign' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignName,
  AdGroupName,
  CampaignStatus,
  AdGroupStatus,
  Status,
  HeadLinePart1,
  HeadLinePart2,
  Description,
  PolicySummary,
  Path1,
  Path2,
FROM ${ pitfallReference( 'ad_path1_missing_in_non_brand_campaign' ) }
UNION ALL
SELECT
  'path2_missing_in_non_brand_campaign' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignName,
  AdGroupName,
  CampaignStatus,
  AdGroupStatus,
  Status,
  HeadLinePart1,
  HeadLinePart2,
  Description,
  PolicySummary,
  Path1,
  Path2,
FROM ${ pitfallReference( 'ad_path2_missing_in_non_brand_campaign' ) }
UNION ALL
SELECT
  'ad_policy_violation' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignName,
  AdGroupName,
  CampaignStatus,
  AdGroupStatus,
  Status,
  HeadLinePart1,
  HeadLinePart2,
  Description,
  PolicySummary,
  Path1,
  Path2,
FROM ${ pitfallReference( 'ad_policy_violation' ) }
UNION ALL
SELECT
  'too_short_description' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignName,
  AdGroupName,
  CampaignStatus,
  AdGroupStatus,
  Status,
  HeadLinePart1,
  HeadLinePart2,
  Description,
  PolicySummary,
  Path1,
  Path2,
FROM ${ pitfallReference( 'ad_too_short_description' ) }
UNION ALL
SELECT
  'too_short_headline' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignName,
  AdGroupName,
  CampaignStatus,
  AdGroupStatus,
  Status,
  HeadLinePart1,
  HeadLinePart2,
  Description,
  PolicySummary,
  Path1,
  Path2,
FROM ${ pitfallReference( 'ad_too_short_headline' ) }
	` ][ 0 ],
	
	KEYWORD : [ `-- keyword
SELECT
  'adgroup_match_type_mismatch' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_adgroup_match_type_mismatch' ) }
UNION ALL
SELECT
  'campaign_match_type_mismatch' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_campaign_match_type_mismatch' ) }
UNION ALL
SELECT
  'contains_capital' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_contains_capital' ) }
UNION ALL
SELECT
  'contains_dash' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_contains_dash' ) }
UNION ALL
SELECT
  'contains_dot' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_contains_dot' ) }
UNION ALL
SELECT
  'modified_negative' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_modified_negative' ) }
UNION ALL
SELECT
  'contains_plus' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_contains_plus' ) }
UNION ALL
SELECT
  'contains_brackets' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_contains_brackets' ) }
UNION ALL
SELECT
  'contains_quotes' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_contains_quotes' ) }
UNION ALL
SELECT
  'session_id_in_url' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_session_id_in_url' ) }
UNION ALL
SELECT
  'target_url_missing' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_target_url_missing' ) }
UNION ALL
SELECT
  'target_url_multiple_question_marks' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_target_url_multiple_question_marks' ) }
UNION ALL
SELECT
  'broad_match_still_active' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_broad_match_still_active' ) }
UNION ALL
SELECT
  'bmm_still_active' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_bmm_still_active' ) }
UNION ALL
SELECT
  'bad_qs' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_bad_qs' ) }
UNION ALL
SELECT
  'below_average_ad_relevance' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_below_average_ad_relevance' ) }
UNION ALL
SELECT
  'below_average_expected_ctr' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_below_average_expected_ctr' ) }
UNION ALL
SELECT
  'below_average_lp_experiance' AS Problem,
  *
FROM ${ pitfallReference( 'keyword_below_average_lp_experiance' ) }
	`][ 0 ],
	
	ADGROUP : [ `-- adgroup
SELECT
  'too_many_keywords' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_too_many_keywords' ) }
UNION ALL
SELECT
  'no_active_keywords' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_no_active_keywords' ) }
UNION ALL
SELECT
  'no_ads' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_no_ads' ) }
UNION ALL
SELECT
  'no_dsa' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_no_dsa' ) }
UNION ALL
SELECT
  'no_keywords' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_no_keywords' ) }
UNION ALL
SELECT
  'no_negative_keywords_in_broad_adgroup' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_no_negative_keywords_in_broad_adgroup' ) }
UNION ALL
SELECT
  'too_few_ads' AS Problem,
   AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignStatus,
  AdGroupStatus,
  CampaignName,
  AdGroupName,
FROM ${ pitfallReference( 'adgroup_too_few_ads' ) }
UNION ALL
SELECT
  'too_few_dsa' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_too_few_dsa' ) }
UNION ALL
SELECT
  'too_many_broad_keywords' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_too_many_broad_keywords' ) }
UNION ALL
SELECT
  'too_many_dsa' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_too_many_dsa' ) }
UNION ALL
SELECT
  'too_many_enabled_ads' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  AdGroupId,
  CampaignStatus,
  AdGroupStatus,
  CampaignName,
  AdGroupName,
FROM ${ pitfallReference( 'adgroup_too_many_enabled_ads' ) }
UNION ALL
SELECT
  'too_many_exact_keywords' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_too_many_exact_keywords' ) }
UNION ALL
SELECT
  'without_audience' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_without_audience' ) }
UNION ALL
SELECT
  'multiple_poor_relevance_keywords' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_multiple_poor_relevance_keywords' ) }
UNION ALL
SELECT
  'no_rsa' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_no_rsa' ) }
UNION ALL
SELECT
  'too_many_rsa' AS Problem,
  *
FROM ${ pitfallReference( 'adgroup_too_many_rsa' ) }
` ][ 0 ],
	
	CAMPAIGN : [ `-- campaign
SELECT
  'missing_mobile_bid_modifier' AS Problem,
  *
FROM ${ pitfallReference( 'campaign_missing_mobile_bid_modifier' ) }
UNION ALL
SELECT
  'non_brand_no_smart_bidding' AS Problem,
  *,
FROM ${ pitfallReference( 'campaign_non_brand_no_smart_bidding' ) }
UNION ALL
SELECT
  'multi_channel' AS Problem,
  *
FROM ${ pitfallReference( 'campaign_multi_channel' ) }
UNION ALL
SELECT
  'non_standard_delivery_method' AS Problem,
  *
FROM ${ pitfallReference( 'campaign_non_standard_delivery_method' ) }
UNION ALL
SELECT
  'rotation_type_not_optimized' AS Problem,
  *
FROM ${ pitfallReference( 'campaign_rotation_type_not_optimized' ) }
UNION ALL
SELECT
  'target_and_bid' AS Problem,
  *
FROM ${ pitfallReference( 'campaign_target_and_bid' ) }
` ][ 0 ],
	
	EXTENSION : [ `-- extension
SELECT
  'no_callouts' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus
FROM ${ pitfallReference( 'extension_no_callouts' ) }
UNION ALL
SELECT
  'no_messages' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_no_messages' ) }
UNION ALL
SELECT
  'no_mobile_apps' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_no_mobile_apps' ) }
UNION ALL
SELECT
  'no_phone_numbers' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_no_phone_numbers' ) }
UNION ALL
SELECT
  'no_site_links' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_no_site_links' ) }
UNION ALL
SELECT
  'no_snippets' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_no_snippets' ) }
UNION ALL
SELECT
  'too_few_callouts' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_too_few_callouts' ) }
UNION ALL
SELECT
  'too_few_site_links' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_too_few_site_links' ) }
UNION ALL
SELECT
  'too_few_snippets' AS Problem,
  AccountName,
  ExternalCustomerId,
  CampaignId,
  CampaignName,
  CampaignStatus,
FROM ${ pitfallReference( 'extension_too_few_snippets' ) }
	`][ 0 ],
};

function createCashedTable( viewRef ){
	Logger.log( 'write results of the view ' + Object.values( viewRef ).join( '.' ) + ' into a table.' );
	queryBigqueryAsync({
		query     : `SELECT * FROM ${ Object.values( viewRef ).join( '.' ) }`,
		reference : {
			...viewRef,
			tableId : CACHED_PREFIX + viewRef.tableId,
		},
		writeDisposition  : 'WRITE_TRUNCATE',
		createDisposition : 'CREATE_IF_NEEDED',
	});
	return viewRef;
}

function createEmptyTable( viewRef ){
	Logger.log( 'create a fake datasource for the view ' + Object.values( viewRef ).join( '.' ) + '.' );
	queryBigqueryAsync({
		query     : `SELECT * FROM ${ Object.values( viewRef ).join( '.' ) } LIMIT 0`,
		reference : {
			...viewRef,
			tableId : 'z_' + viewRef.tableId,
		},
		writeDisposition  : 'WRITE_TRUNCATE',
		createDisposition : 'CREATE_IF_NEEDED',
	});
	return viewRef;
}

function getEarliestDateInDatabase(){
	if( ! tableOrViewExists( BQ.ANALYTICS_DATA ) ){
		Logger.log( 'Can\'t import analytics data because the Bigquery table is not created yet' );
		return;
	}
	let query = `SELECT MIN( _PARTITIONDATE ) AS start, FROM \`${ Object.values( BQ.ANALYTICS_DATA ).join( '.' ) }\``;
	let earliestDate = queryTableOrView({
			query : query,
			projectId : BQ.ANALYTICS_DATA.projectId
	})[ 0 ].start || Utilities.formatDate( new Date(), 'GMT', 'yyyy-MM-dd' );
	//Logger.log( earliestDate );
	return new Date( earliestDate );
}

function importGaData2(){
	let stopDate = getEarliestDateInDatabase();
	Logger.log( 'earliest date in db: ' + Utilities.formatDate( stopDate,  'GMT', 'yyyy-MM-dd' ) );
	let daysInDatabase = Math.round( ( new Date().getTime() - stopDate.getTime() ) / ( 24 * 60 * 60 * 1000 ) );
	Logger.log( 'daysInDatabase: ' + daysInDatabase );
	if( daysInDatabase <= MAX_DAYS_OF_ANALYTICS_DATA ){
		Logger.log( 'Get data from the past' );
		importGaData({ stopDate : stopDate, days : LIMIT_ANALYTICS_DATA_PER_EXECUTION_IN_DAYS });
	}
	Logger.log( 'Get data for yesterday' );
	importGaData({ stopDate : new Date(), days : 1 });
}

function importGaData({ stopDate, days }){
	if( ! stopDate ){
		throw new Error( 'stopDate must be supplied' );
	}
	let startDate = new Date( stopDate.getTime() - days * 24 * 60 * 60 * 1000 );
	
	gaGet({
		viewIds   : GOOGLE_ANALYTICS_VIEW_IDS,
		startDate : Utilities.formatDate( startDate, 'GMT', 'yyyy-MM-dd' ),
		stopDate  : Utilities.formatDate( stopDate,  'GMT', 'yyyy-MM-dd' ),
		metrics   : [
			'ga:transactions',
			'ga:transactionRevenue',
			'ga:sessions',
			'ga:goal1Completions',
			'ga:goal2Completions',
			'ga:goal3Completions',
			'ga:goal4Completions',
			'ga:goal5Completions',
			'ga:goal6Completions',
			'ga:goal7Completions',
			'ga:goal8Completions',
			'ga:goal9Completions',
			'ga:goal10Completions',
			'ga:goal11Completions',
			'ga:goal12Completions',
			'ga:goal13Completions',
			'ga:goal14Completions',
			'ga:goal15Completions',
			'ga:goal16Completions',
			'ga:goal17Completions',
			'ga:goal18Completions',
			'ga:goal19Completions',
			'ga:goal20Completions',
			'ga:goalCompletionsAll',
		],
		options   : {
			segment       : 'users::condition::ga:sourceMedium==google / cpc',
			dimensions    : [
				'ga:date',
				'ga:adwordsCustomerID',
				'ga:adwordsCampaignID',
				'ga:adwordsAdgroupID',
				'ga:adwordsCriteriaId',
				'ga:adMatchType',
				'ga:adMatchedQuery',
			].join( ',' ),
			samplingLevel : 'HIGHER_PRECISION',
			filters       : [],
		},
	})
		.filter( nonEmpty )
		.forEach( result =>
			bqLoad({
				reference : {
					...BQ.STAY_IN_CONTROL_DATASET,
					tableId   : 'analytics_data' + '$' + result[ 0 ].date.replace( /-/g, '' ),
				},
				data               : result,
				writeDisposition   : 'WRITE_TRUNCATE',
			})
			.waitForJobToFinish( 1000 * 5 )
			.print()
	);
}

function range( start, stop, step = 1 ){
  return Array.from( { length: ( stop - start ) / step }, ( _, i ) => start + ( i * step ) );
}

function dateRange( startDate, stopDate, step = 1 ){
	return range(
		new Date( startDate ).getTime(),
		new Date( stopDate ).getTime(),
		step * 24 * 60 * 60 * 1000
	).map( millis => Utilities.formatDate( new Date( millis ), 'GMT', 'yyyy-MM-dd' ) );
}

function dateToString( date ){
    function pad( x, length ){
        x = '' + x;
        while( x.length < length ){
            x = '0' + x;
        }
        return x;
    }
    var year  = date.getFullYear();
    var month = date.getMonth() + 1;
    var day   = date.getDate();
    return pad( year, 4 ) + '-' + pad( month, 2 ) + '-' + pad( day, 2 );
}

function gaGet({ viewIds, startDate, stopDate, metrics, options }){
	function setProperty( obj, { name, dataType, columnType }, value ){
		name = name.slice( 3 ); // remove the 'ga:' prefix
		// columnType IN ( DIMENSION, METRIC )
		// dataType IN ( STRING, INTEGER, CURRENCY, ... ? )
		if( name == 'date' ){
			value = value.substring( 0, 4 ) + '-' + value.substring( 4, 6 ) + '-' + value.substring( 6, 8 );
		}
		obj[ name ] = value;
		return obj;
	}
	
	function getRowsFromGa( date ){
		Logger.log( date );
		return function({ metricSet, viewId }){
			let x = {};
			try{
				x = Analytics.Data.Ga.get(
					viewId,
					date, // startDate
					date, // stopDate
					metricSet.join( ',' ),
					{
						'start-index' : 1, // 1-based. TODO: if there are more than 10k rows then we need to repeat the request with start-index = 10001, 20001,...
						'max-results' : MAX_RESULTS, // 10k is the maximum allowed value. default is 1k
						...options,
					}
				);
			}catch( e ){
				if( e.message && 
					[
						'There was a temporary error. Please try again later.',
						'There was an internal error',
					].map( part => e.message.includes( part ) )
					.reduce( ( a, b ) => a || b, false )
					){
					Logger.log( 'analytics internal/temporary error. Skip.' );
					// ignore
				}else{
					throw e;
				}
			}
			//Logger.log( 'items per page: ' + x.itemsPerPage );
			let rows = x.rows || [];
			if( rows.length >= MAX_RESULTS ){
				// we want to know when we hit the limit
				Logger.log( 'rows: ' + rows.length );
			}
			return rows.map(
				row =>
					row.reduce( ( obj, value, index ) =>
						setProperty(
							obj,
							x.columnHeaders[ index ],
							value
						),
						{}
					)
			);
		};
	}
	
	return dateRange( startDate, stopDate ).map( date =>
		metrics
			.reduce( partition( MAX_ALLOWED_METRICS ), [] )
			// cartesian product of metricSets and viewIds
			.flatMap( metricSet => viewIds.map( viewId => ({ metricSet : metricSet, viewId : viewId }) ) )
			.flatMap( getRowsFromGa( date ) )
			.reduce( merge(), [] )
	);
}

/**
	merge items based on common keys.
	Example: [ { a : 1, b : 2 }, { a : 1, c : 3 }, { a : 2 } ].reduce( merge(), [] ) ===> [ { a : 1, b : 2, c : 3 }, { a : 2 } ]
*/
function merge(){
	let commonKeys;
	let mergedItems = {};
	
	return function( resultArr, item, index, arr ){
		if( typeof commonKeys == 'undefined' ){
			let allKeys = arr
				.flatMap( Object.keys )
				.filter( onlyUnique() )
			;
			commonKeys = arr
				.map( Object.keys )
				.reduce(
					( keys, itemKeys ) => keys.filter( key => itemKeys.includes( key ) ),
					allKeys
			);
			if( commonKeys.length == 0 ){
				throw new Error( 'no common keys found' );
			}
			//console.log( 'common keys: ' + commonKeys );
		}
		
		// all items with the same bigKey are going to be merged into one item
		let bigKey = commonKeys.map( key => item[ key ] ).join( ',' );
		
		// initalize with empty obj
		mergedItems[ bigKey ] = mergedItems[ bigKey ] || {};
		// copy item into mergedItems. Potentially overrides values.
		Object.keys( item )
			.forEach( key => ( mergedItems[ bigKey ][ key ] = item[ key ] ) )
		;
		
		return Object.values( mergedItems );
	};
}

function createTablesAndViewsConsentChecker(){
	createDataset( BQ.CONSENT_CHECKER_DATASET, BQ.LOCATION );
	
	createTable({ // cookie_consent_check
		reference            : BQ.CONSENT_CHECKER_TABLE,
		fields               : [
			{
				name : 'timestamp',
				type : 'TIMESTAMP',
				mode : 'NULLABLE',
			},
			{
				name : 'URI',
				type : 'STRING',
				mode : 'NULLABLE',
			},
		],
	});
	
	createOrUpdateView({
		reference : BQ.CONSENT_CHECKER_VIEW,
		query     : CONSENT_CHECKER_QUERY,
	});
	
	createCashedTable( BQ.CONSENT_CHECKER_VIEW );
	createEmptyTable(  BQ.CONSENT_CHECKER_VIEW );
}


function createTablesAndViewsStayInControl(){
	createDataset( BQ.STAY_IN_CONTROL_DATASET, BQ.LOCATION );
	
	createTable({ // analytics_data
		reference            : BQ.ANALYTICS_DATA,
		timePartitioningType : 'DAY',
		fields               : [
			{
				name : 'adwordsCustomerID',
				type : 'STRING',
				mode : 'NULLABLE',
			},
			{
				name : 'adwordsCampaignID',
				type : 'STRING',
				mode : 'NULLABLE',
			},
			{
				name : 'adwordsAdgroupID',
				type : 'STRING',
				mode : 'NULLABLE',
			},
			{
				name : 'adwordsCriteriaId',
				type : 'STRING',
				mode : 'NULLABLE',
			},
			{
				name : 'adMatchType',
				type : 'STRING',
				mode : 'NULLABLE',
			},
			{
				name : 'adMatchedQuery',
				type : 'STRING',
				mode : 'NULLABLE',
			},
			{
				name : 'date',
				type : 'DATE',
				mode : 'NULLABLE',
			},
			{
				name : 'transactions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'transactionRevenue',
				type : 'FLOAT',
				mode : 'NULLABLE',
			},
			{
				name : 'sessions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goalCompletionsAll',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal1Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal2Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal3Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal4Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal5Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal6Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal7Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal8Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal9Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal10Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal11Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal12Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal13Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal14Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal15Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal16Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal17Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal18Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal19Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
			{
				name : 'goal20Completions',
				type : 'INTEGER',
				mode : 'NULLABLE',
			},
		],
	});
	
	STAY_IN_CONTROL_VIEWS.forEach( createOrUpdateView );
	
	let viewsForDatastudio = [
		BQ.GHOST_TERMS,
		BQ.GHOST_TERMS_BY_AD_NETWORK,
		BQ.GHOST_NGRAMS,
		BQ.MISSING,
		BQ.ROUTING_PROBLEMS,
		BQ.COUNTS,
		BQ.NGRAM,
		BQ.RAW_DATA,
	];
	viewsForDatastudio
		.filter(
			ref => ({ ...ref, tableId : CACHED_PREFIX + ref.tableId }),
			lastModifiedInHours,
			hours => hours > CACHE_EVERY_HOURS
		) // don't do caching too often
		.forEach( createCashedTable )
	;
	viewsForDatastudio.forEach( createEmptyTable  );
	
}


function createTablesAndViewsPitfall(){
	createDataset(
		PITFALLS_DATASET,
		BQ.LOCATION
	);
	
	Object.entries( COMMON_PITFALLS )
		//.filter( ([ name, query ]) => !PITFALLS_DEPENDING_ON_AUGMENTER_SCRIPT.includes( name ) )
		.map( ([ name, query ]) => ({
			reference : {
				...PITFALLS_DATASET,
				tableId : PITFALL_PREFIX + name.toLowerCase(),
			},
			query : query,
		}))
		.map( createOrUpdateView )
		.filter( 'tableId', isNotIncludedIn( ABSORBED_PITFALLS ) )
		.filter(
			ref => ({ ...ref, tableId : CACHED_PREFIX + ref.tableId }),
			lastModifiedInHours,
			hours => hours > CACHE_EVERY_HOURS
		) // don't do caching too often
		.map( createCashedTable )
		.map( createEmptyTable )
	;
}







function currentDate( delimiter, delta ){
	delimiter = ( typeof delimiter == 'undefined' ? '-' : delimiter );
	const now = new Date( new Date().getTime() + 1000 * 60 * 60 * 24 * ( delta || 0 ) );
	const time =
		now.getFullYear()       .toString().padStart( 4, '0' ) + delimiter
		+ ( now.getMonth() + 1 ).toString().padStart( 2, '0' ) + delimiter
		+ now.getDate()         .toString().padStart( 2, '0' )
		//+ ' '
		//+ now.getHours()        .toString().padStart( 2, '0' ) + ':'
		//+ now.getMinutes()      .toString().padStart( 2, '0' ) + ':'
		//+ now.getSeconds()      .toString().padStart( 2, '0' )
	;
	return time;
}

function currentDateTime( delimiter, delta ){
	delimiter = ( typeof delimiter == 'undefined' ? '-' : delimiter );
	const now = new Date( new Date().getTime() + 1000 * 60 * 60 * 24 * ( delta || 0 ) );
	const time =
		now.getFullYear()       .toString().padStart( 4, '0' ) + delimiter
		+ ( now.getMonth() + 1 ).toString().padStart( 2, '0' ) + delimiter
		+ now.getDate()         .toString().padStart( 2, '0' )
		+ ' '
		+ now.getHours()        .toString().padStart( 2, '0' ) + ':'
		+ now.getMinutes()      .toString().padStart( 2, '0' ) + ':'
		+ now.getSeconds()      .toString().padStart( 2, '0' )
	;
	return time;
}


/**Flatten a multidimensional object
 * 
 *
 * For example:
 *   flattenObject({ a: 1, b: { c: 2 } })
 * Returns:
 *   { a: 1, c: 2 }
 */
function flattenObject( obj ){
	return function _flatten( o ){
		return Object.entries( o ).reduce( ( result, [ k, v ] ) =>
			typeof v === 'object' ? Object.assign( result, _flatten( v ) ) : ( result[ k ] = v, result ),
			{}
			)
		;
	}( obj )
}

function camalize( str ){
    return str.toLowerCase().replace( /[^a-zA-Z0-9]+(.)/g, ( m, chr ) => chr.toUpperCase() );
}


function group( rows ){
	if( ! rows ){
		throw new Error( 'rows is undefined' );
	}
	if( ! Array.isArray( rows ) ){
		throw new Error( 'rows must be an array, but it is ' + ( typeof rows ) );	
	}
	function property(){
		var args = Array.prototype.slice.call( arguments );
		return function( item, index, array ){
			// do NOT use reduce here, because apply will interpret the third argument :(
			var res = item;
			args.forEach( function( arg ){ res = apply( res, arg, index, array ) } );
			return res;
		};
	}
	
	return {
		byAll : function( keys, finalKey ){
			function recursive( keys, res, row ){
				var key = keys[ 0 ];
				var value = row[ key ];
				
				var otherKeys = keys.slice( 1 );
				if( otherKeys.length > 0 ){
					res[ value ] = res[ value ] || {};
					recursive( otherKeys, res[ value ], row );
				}else{
					if( finalKey ){
						res[ value ] = row[ finalKey ];
					}else{
						res[ value ] = row;	
					}
				}
			}
			var res = {};
			rows.forEach( function( row ){ recursive( keys, res, row ) } );
			return res;
		},
		by : function(){
			var keyMapper = property.apply( null, arguments );
			return {
				avg : function( value ){
					var res = {};
					let count = {};
					rows.forEach( function( row ){
						var key = keyMapper( row );
						count[ key ] = ( count[ key ] || 0 ) + 1;
						res[ key ]   = ( ( res[ key ] || 0 ) * ( count[ key ] - 1 ) + row[ value ] / 1 ) / count[ key ];
					});
					return res;
				},
				sum : function( value ){
					var res = {};
					rows.forEach( function( row ){
						var key = keyMapper( row );
						res[ key ] = ( res[ key ] || 0 ) + row[ value ];
					});
					return res;
				},
				count : function(){
					var res = {};
					rows.forEach( function( row ){
						var key = keyMapper( row );
						res[ key ] = ( res[ key ] || 0 ) + 1;
					});
					return res;
				},
				any : function(){
					var res = {};
					var valueMapper = property.apply( null, arguments );
					rows.forEach( function( row ){
						var key = keyMapper( row );
						var value = apply( row, valueMapper );
						res[ key ] = value;
					});
					return res;
				},
				all : function( valueMapper ){
					var res = {};
					valueMapper = ( typeof valueMapper != 'undefined' )
						? valueMapper
						: function( x ){ return x }
					;
					rows.forEach( function( row ){
						var key = keyMapper( row );
						res[ key ] = res[ key ] || [];
						var value = apply( row, valueMapper );
						res[ key ].push( value );
					});
					return res;
				},
			};
		}
	};
}

function toList( iter ){
	let res = [];
	while( iter.hasNext() ){
		res.push( iter.next() );
	}
	return res;
}

// ----- Functionals ------------

function constant( value ){ return () => value }

function assign( source ){
	return item => Object.assign( item, source );
}

function toObj( name ){
	return item => ({ [name] : item });
}

function if_( condition ){
	return {
		then  : mapper => condition ? mapper : x => x,
		then_ : mapper => ({
			else_ : other => condition ? mapper : other,
		}),
	};
}

function typeOf( attr ){
	return item => typeof item[ attr ];
}

// ----- Math ------------------

function isPowerOf2( v ){
    return v > 0 && !( v & ( v - 1 ) );
}

// Math.max is too generic to be used directly
function max( a, b ){ return  Math.max( a, b ) }

// Math.min is too generic to be used directly
function min( a, b ){ return  Math.min( a, b ) }

function minus( delta ){ return value => value - delta }

function plus( delta ){ return value => value + delta }

function divideBy( divisor ){ return value => value / ( divisor || 1 ) }

function times( factor ){ return value => value * factor }

// ------ Booleans -------------

function or( a, b ){ return item => item[ a ] || item[ b ] }

function equals( other ){ return item => item == other }

function not( predicate ){ return item => ! apply( item, predicate ) }

function isDefined( value ){
	if( typeof value == 'undefined' ){
		return item => item != null;
	}
	return item => item[ value ] != null;
}

function nonEmpty( item ){
	return item !== null && item && item.length && item.length > 0;
}

function isEmpty( item ){
	return !nonEmpty( item );
}

function has( attribute ){ return item => typeof item[ attribute ] != 'undefined' }

function onlyUnique(){ // arguments are used
	const memory = [];
	
	const mapperList = [].slice.call( arguments );
	const finalMapper = function( item, index, array ){
		var res = item;
		mapperList.forEach( function( mapper ){
			res = apply( res, mapper, index, array );
		});
		return res;
	};
	return function( value, index, array ){
		const mappedValue = apply( value, finalMapper, index, array );
		const notFound = memory.indexOf( mappedValue ) == -1;
		if( notFound ){
			memory.push( mappedValue );
		}
		return notFound;
	};
}

function isIncludedIn( arr ){
	if( ! Array.isArray( arr ) ){
		arr = [].slice.call( arguments );
	}
	return item => arr.includes( item );
}

function isNotIncludedIn( arr ){
	const func = isIncludedIn( arr );
	return item => !func( item );
}

function startsWith( str ){
	return item => item.startsWith( str );
}

// ------ Strings --------------

function toString(){
	const args = [].slice.call( arguments );
	return item => {
		return args.map( arg => typeof item[ arg ] != 'undefined' ? item[ arg ] : arg ).join( '' );
	}
}

function join( delimiter, delimiter2 ){
	return arr => {
		if( Array.isArray( arr ) ){
			return arr.join( delimiter );
		}
		return Object.entries( arr ).map( join( delimiter ) ).join( delimiter2 || '\n' );
	};
}

function partition( partitionSize ){
	// how to use: keywords.reduce( partition( 100 ), [] )
	if( typeof partitionSize == 'undefined' ){
		// assume infinete parition size
		partitionSize = Infinity;
	}
	return ( arr, keyword ) => ( arr.length > 0 && arr[ arr.length - 1 ].length < partitionSize ? arr[ arr.length - 1 ].push( keyword ) : arr.push( [ keyword ] ), arr );
}

// ----- END Functionals -------









// ------ Enhance Map/Filter/Reduce --------

function apply( item, mapper, index, array ){
	if( mapper instanceof RegExp ){
		if( typeof item == 'string' ){
			return ( item.match( mapper ) || [] );
		}
		throw new Error( 'apply() got a regex mapper for a non-string item' );
	}
	
	if(
		typeof item == 'object'
		&& Object.prototype.toString.call( item ) == '[object Array]'
		&& typeof mapper == 'number'
	){
		return item[ mapper ];
	}
	
	if( typeof mapper == 'function' ){
		return mapper( item, index, array );
	}
	if(
		typeof mapper == 'string'
		&& typeof item[ mapper ] == 'function'
	){
		return item[ mapper ]();
	}
	if(
		( typeof mapper == 'string' || typeof mapper == 'number' )
		&& typeof item[ mapper ] != 'undefined'
	){
		return item[ mapper ];
	}
	
	if( typeof mapper[ item ] != 'undefined' ){
		return mapper[ item ];
	}
	
	if(
		typeof mapper == 'object'
		&& Object.prototype.toString.call( mapper ) == '[object Object]'
	){
		//console.log( 'obj' );
		var res = {};
		Object.keys( mapper ).forEach( function( key ){
			res[ key ] = mapper[ key ] === null ? null : apply( item, mapper[ key ] );
		});
		return res;
	}
	
	if(
		typeof mapper == 'object'
		&& Object.prototype.toString.call( mapper ) == '[object Array]'
	){
		//console.log( 'arr' );
		return mapper.map( function( mapperX ){
			return apply( item, mapperX, index, array );
		});
	}
	
	throw new Error(
		'apply() can\'t determine what to do with '
		+ JSON.stringify( item, null, 2 )
		+ ' and '
		+ mapper
	);
}

// --------- Enhance Functionals ----------
Array.prototype.map    = enhanceFunctional( Array.prototype.map );
Array.prototype.filter = enhanceFunctional( Array.prototype.filter );
Array.prototype.flatten = function(){
	return this.reduce( ( a, b ) => a.concat( b ), [] );
};
Array.prototype.partition = function( partitionSize ){
	if( partitionSize === 0 ){
		return this;
	}
	if( typeof partitionSize == 'undefined' ){
		// assume infinete parition size
		partitionSize = Infinity;
	}
	if( this.length == 0 ){
		// reduce can't hanlde this case
		return [[]];
	}
	return this.reduce( ( arr, keyword ) => ( arr.length > 0 && arr[ arr.length - 1 ].length < partitionSize ? arr[ arr.length - 1 ].push( keyword ) : arr.push( [ keyword ] ), arr ), [] );
};
Array.prototype.handle = function( consumer ){
	consumer( this );
	return this;
};

function enhanceFunctional( originalFunctional ){
	return function(){
		var mapperList = [].slice.call( arguments );
		var finalMapper = function( item, index, array ){
			var res = item;
			mapperList.forEach( function( mapper ){
				res = apply( res, mapper, index, array );
			} );
			return res;
		};
		return originalFunctional.call( this, finalMapper );
	};
}

// --------- END Enhance Map/Filter/Reduce -----



// ####################################################
// ####################################################

function sendEmail( recipient, subject, text, html ){
	if( !text && !html ){
		throw new Error( 'Whether text-body nor html supplied for email.' );
	}
	if( MAILGUN.SEND_EMAILS_THROUGH_MAILGUN ){
		return mailGunSender( recipient, subject, text, html );
	}
	mailAppSender( recipient, subject, text, html );
}

function mailAppSender( recipient, subject, text, html ){
	MailApp.sendEmail(
		recipient,
		subject,
		text,
		{
			name     : SCRIPT_NAME,
			htmlBody : html
		}
	);
}

function mailGunSender( recipient, subject, text, html ){
	if ( html ){
		if ( !text ){
			text = 'this is supposed to be a html email. Seems like your device doesn\'t support html emails.';
		}
		//html = '<html><body>' + html + '</body></html>';
	} else {
		html = null;
	}
	Logger.log( 'fetch URL' );

	return UrlFetchApp.fetch(
		MAILGUN.URL,
		{
			method      : 'post',
			payload     : {
				from    : MAILGUN.FROM,
				to      : recipient,
				subject : subject,
				text    : text,
				html    : html,
			},
			headers     : {
				Authorization : MAILGUN.AUTHORISATION,
			}
		}
	 );
}





// ----- BigQuery ----------------

function lastModifiedInHours( reference ){
  if( typeof BigQuery == 'undefined' ){ throw new Error( 'BigQuery is not defined' ) }
	let table = BigQuery.Tables.get( reference.projectId, reference.datasetId, reference.tableId );
  let diff  = Math.round( ( new Date().getTime() - table.lastModifiedTime ) / 1000 / 60 / 60 );
  //Logger.log( diff );
  return diff;
}

function bqLoad({ reference, data, writeDisposition, noTimePartitioning, exceptionHandler }){
	const reference1 = {
		projectId : BQ.PROJECT_ID,
		...reference,
	};
	
	const resource = {
		configuration    : {
			jobType      : 'LOAD',
			load         : {
				destinationTable    : reference1,
				//schema            : { fields : [] },
				autodetect	        : true,
				createDisposition   : 'CREATE_IF_NEEDED',
				writeDisposition    : writeDisposition || 'WRITE_APPEND',
				sourceFormat        : 'NEWLINE_DELIMITED_JSON',
				//nullMarker        : string,
				//fieldDelimiter    : string,
				//skipLeadingRows   : integer,
				//encoding          : string,
				//quote             : string,
				//maxBadRecords     : integer,
				//schemaInlineFormat: string,
				//schemaInline      : string,
				//allowQuotedNewlines: boolean,
				//allowJaggedRows: boolean,
				//ignoreUnknownValues: boolean,
			},
			//dryRun       : false,
			//jobTimeoutMs : '',
		},
	};
	
	if( resource.configuration.load.writeDisposition != 'WRITE_TRUNCATE' ){
		// preventing "Schema update options should only be specified with WRITE_APPEND disposition, or with WRITE_TRUNCATE disposition on a table partition."
		resource.configuration.load.schemaUpdateOptions = [
			'ALLOW_FIELD_ADDITION',
			'ALLOW_FIELD_RELAXATION',
		];
	}
	
	if( tableOrViewExists( reference1 ) ){
		const table = BigQuery.Tables.get( reference1.projectId, reference1.datasetId, reference1.tableId );
		resource.configuration.load.schema = table.schema;
	}else{
		resource.configuration.load.autodetect = true;
		if( ! noTimePartitioning ){
			resource.configuration.load.timePartitioning = {
				type                   : 'DAY',
				//field                  : string,
				//requirePartitionFilter : boolean
			};
			if( typeof EXPIRATION_MS != 'undefined' ){
				resource.configuration.load.timePartitioning.expirationMs = EXPIRATION_MS;
			}
		}
	}
	
	try{
		const newLineDelimitedJSON = data.map( row => JSON.stringify( row ) ).join( '\n' );
		//Logger.log( 'new line delimited json: ' + newLineDelimitedJSON );
		
		const job = BigQuery.Jobs.insert(
			resource,
			reference1.projectId,
			Utilities.newBlob( newLineDelimitedJSON )
		);
		return {
			job                : job,
			waitForJobToFinish : seconds => waitForJobToFinish( job.id, seconds ),
		};
	}catch( error ){
		if( typeof exceptionHandler == 'function' ){
			return exceptionHandler( error );
		}else{
			throw error;
		}
	}
}

function createDataset( reference, location ){
	if( typeof arguments[ 0 ] == 'string' ){
		reference = {
			projectId : BQ.PROJECT_ID,
			datasetId : arguments[ 0 ],
		};
	}
	if( typeof arguments[ 1 ] != 'string' ){
		location = BQ.LOCATION;
	}
	if( datasetExists( reference ) ){
		//Logger.log( 'Dataset ' + reference.datasetId + ' in project ' + reference.projectId + ' already exists. Don\'t recreate it.' );
		return;
	}
	
	const dataSet = BigQuery.Datasets.insert(
		{
			friendlyName     : reference.datasetId,
			location         : location,
			datasetReference : reference,
		},
		reference.projectId
	);
	Logger.log( 'Created dataset with id ' + dataSet.id + '.' );
}

function deleteDataset( reference, forceDelete ){
	if( typeof arguments[ 0 ] == 'string' ){
		reference = {
			projectId : BQ.PROJECT_ID,
			datasetId : arguments[ 0 ],
		};
	}
	if( !datasetExists( reference ) ){
		Logger.log( 'No dataset ' + reference.datasetId + ' in project ' + reference.projectId + ' found. Nothing to delete.' );
		return;
	}
	
	BigQuery.Datasets.remove.apply(
		BigQuery.Datasets,
		[
			...Object.values( reference ),
			{ deleteContents : forceDelete || false }
		]
	);
	Logger.log( 'Dataset ' + reference.datasetId + ' deleted.' );
}

function datasetExists( reference ){
	return bq.getDatasets( { projectId : reference.projectId } )
		.map( 'datasetReference' )
		.map( 'datasetId' )
		.includes( reference.datasetId )
	;
}

function createTable({
			reference,
			location = BQ.LOCATION,
			description = '',
			fields,
			timePartitioningType,//'DAY'
			timePartitioningField,
			timePartitioningExpirationMs,
			requirePartitionFilter,
			externalDataConfiguration,
			
		}){
	if( tableOrViewExists( reference ) ){
		return;	
	}
	
	var table = {
		friendlyName   : reference.tableId,
		description    : description,
		location       : location,
		//schema         : { fields : fields },
		tableReference : reference,
	};
	if( fields ){
		table.schema = { fields : fields };
	}
	
	if( externalDataConfiguration ){
		table.externalDataConfiguration = {
			sourceFormat        : 'GOOGLE_SHEETS',
			googleSheetsOptions : {
				range           : externalDataConfiguration.range,
				skipLeadingRows : externalDataConfiguration.skipLeadingRows || 0,
			},
			sourceUris          : externalDataConfiguration.sourceUris,
			maxBadRecords       : externalDataConfiguration.maxBadRecords || 0,
			ignoreUnknownValues : externalDataConfiguration.ignoreUnknownValues || false,
			
		};
		if( typeof externalDataConfiguration.autodetect == 'boolean' ){
			table.externalDataConfiguration.autodetect = externalDataConfiguration.autodetect;
		}
	}
	
	if( timePartitioningType ){
		table.timePartitioning = {
			type : timePartitioningType,
		};
		if( timePartitioningField ){
			table.timePartitioning.field = timePartitioningField;
		}
		if( timePartitioningExpirationMs ){
			table.timePartitioning.expirationMs = timePartitioningExpirationMs;
		}
		if( requirePartitionFilter ){
			table.requirePartitionFilter = requirePartitionFilter;
		}
	}
	
	try{
		
		const table2 = BigQuery.Tables.insert.apply(
			BigQuery.Tables,
			[
				table,
				reference.projectId,
				reference.datasetId
			]
		);
		Logger.log( 'Table ' + reference.tableId + ' created.' );
		return table2;
	}catch( error ){
		Logger.log( '----------------------> ' + error + ' - ' + reference.tableId );
		throw error;
	}
}

function createOrUpdateView({ reference, query }){
	function executeFailable( obj, method, args ){
		try{
			return obj[ method ].apply( obj, args );
		}catch( error ){
			if( error?.details?.message?.startsWith( 'Not found' ) ){
				Logger.log( error.details.message );
				return null; 
			}
			throw error;
		}
	}
	
	let table = executeFailable( BigQuery.Tables, 'get', [ reference.projectId, reference.datasetId, reference.tableId ] );
	
	if( table && table.view.query != query ){
		Logger.log( `query ${reference.tableId} seems to be updated. Drop the old view.` );
		BigQuery.Tables.remove( reference.projectId, reference.datasetId, reference.tableId );
	}
	if( !table || table.view.query != query ){
		Logger.log( 'create view ' + Object.values( reference ).join( '.' ) );
		BigQuery.Tables.insert(
			{
				tableReference   : reference,
				view : {
					query        : query,
					useLegacySql : false,
				}
			},
			reference.projectId,
			reference.datasetId
		);
	}
	return reference;
}

function createView({
			reference,
			query,
			replaceView = true,
		}){
	if ( tableOrViewExists( reference ) ){
		if( replaceView ){
			Logger.log( 'Recreate view ' + reference.tableId );
			dropViewOrTable( reference );
		}else{
			Logger.log( 'View ' + reference.tableId + ' already exists. Don\'t recreate it.' );
			return;	
		}
	}

	const table = {
		friendlyName     : reference.tableId,
		tableReference   : reference,
		view : {
			query        : query,
			useLegacySql : false,
		}
	};
	
	try{
		const view = BigQuery.Tables.insert.apply(
			BigQuery.Tables,
			[
				table,
				reference.projectId,
				reference.datasetId,
			]
		);
		Logger.log( 'View ' + reference.tableId + ' created.' );
		
		return view;
	}catch( error ){
		Logger.log( '----------------------> ' + error + ' - ' + reference.tableId );
		throw error;
	}
}

function dropViewOrTable( reference ){
	if ( tableOrViewExists( reference ) ){
		BigQuery.Tables.remove.apply( BigQuery.Tables, Object.values( reference ) );
		Logger.log( 'View/Table ' + reference.tableId + ' dropped.' );
	}
}

function tableOrViewExists( reference ){
	if( typeof BigQuery == 'undefined' ){ throw new Error( 'BigQuery is not defined' ) }
	try{
		BigQuery.Tables.get( reference.projectId, reference.datasetId, reference.tableId );
		return true;
	}catch( error ){
		if( error.details && error.details.message && error.details.message.startsWith( 'Not found' ) ){
			return false; 
		}
		throw error;
		//Logger.log( 'details: ' + JSON.stringify( error.details, null, 2 ) );
	}
}

/*
	Strips "f" and "v" objects/properties from Bigquery query result.
*/
function stripUselessBoilerplate( x ){
	if( x === null || typeof x != 'object' ){
		return x; // scalar
	}
	return ( x.f || x )
		.map( 'v' )
		.map( stripUselessBoilerplate )
	;
}

function toMatrix( arr ){
	if( arr.length == 0 ){
		throw new Error( 'can\'t convert empty arr to matrix' );
	}
	return [ Object.keys( arr[ 0 ] ) ].concat( arr.map( Object.values ) );
}

const bq = {
	getDatasets : BigQueryOperation( 'Datasets' ),
	getRoutines : BigQueryOperation( 'Routines' ),
	getTables   : BigQueryOperation( 'Tables'   ),
	// -------------------
	listTableData   : BigQueryOperation( 'Tabledata' ),
	getQueryResults : BigQueryOperation( 'Jobs'      ),
	
}

function getQueryResultsExperemental( job ){
	const INITIAL_WAIT_TIME = 500;
	const PROJECT_ID = 'bidd-io';
	const jobId = job.jobReference.jobId;
	// Wait until job is done.
	for( let n = 1; ( BigQuery.Jobs.get( PROJECT_ID, jobId ).status || {} ).state != 'DONE'; n *= 2 ){
		Utilities.sleep( INITIAL_WAIT_TIME * n );
	}
	function* getNextPage( jobId ){
		do{
			( { pageToken, rows } = BigQuery.Jobs.getQueryResults(
				PROJECT_ID,
				jobId,
				{ pageToken : typeof pageToken != 'undefined' ? pageToken : null }
			) );
			yield rows;
		}while( pageToken );
	}
	return Array.from( getNextPage( jobId ) )
		.reduce( ( a, b ) => a.concat( b ), [] )
		.map( row => row.f.map( item => item.v ) )
	;
}


function BigQueryOperation( dataType ){
	function parseRow( schema ){
		return function( x ){
			//Logger.log( 'x: ' + JSON.stringify( x, null, 2 ) );
			//Logger.log( 'schema: ' + JSON.stringify( schema, null, 2 ) );
			//Logger.log( '------' );
			
			if( typeof schema == 'undefined' ){
				throw new Error( 'schema is undefined, x is ' + JSON.stringify( x, null, 2 ) );
			}
			if( ! Array.isArray( x ) ){ // scalar
				return {
					name : schema.name,
					value : x,
				};
			}
			// x is an array ( Record or Repeated ? )
			if( Array.isArray( schema ) ){ // zip to an object
				if( schema.length != x.length ){
					throw new Error( 'lenghts differ: ' + schema.length + ', ' +  x.length + '. ' + schema + ' <==> ' + x );
				}
				var arr = [];
				for( var i = 0; i < schema.length; i++ ){
					arr.push( parseRow( schema[ i ] )( x[ i ] ) );
				}
				var obj = {};
				arr.forEach( function( y ){
					obj[ y[ 'name' ] ] = y[ 'value' ];
				});
				return obj;
			}
			if( schema.mode == 'REPEATED' ){ // list of objects
				if( schema.fields ){
					return {
						name : schema.name,
						value : x.map( parseRow( schema.fields ) ),
					};
				}
				return { // list of scalars
					name : schema.name,
					value : x,
				};
			}
			if( schema.type == 'RECORD' ){
				return {
					name  : schema.name,
					value : parseRow( schema.fields )( x )
				};
			}
			throw new Error( 'x is an array, but schema is not: ' + JSON.stringify( x, null, 2 ) + ' <-> ' + JSON.stringify( schema, null, 2 ) );
		}
	}
	return reference => {
		
		let pageToken; // start with empty pageToken
		let results;
		let rows = [];
		//Logger.log( 'refernce: '   + JSON.stringify( reference,  null, 2 ) );
		do{
			const endpoint = BigQuery[ dataType ];
			results = ( endpoint.getQueryResults || endpoint.list ).apply(
				endpoint,
				[
					...Object.values( reference ),
					{
						pageToken : pageToken,
						//maxResults : 1, // sometimes maxResults is for 1 page and sometimes for all pages. :(
					}
				]
			);
			rows = rows.concat( results.rows || results[ dataType.toLowerCase() ] || [] );
			pageToken = results.nextPageToken || results.pageToken;
			/*
			Object.entries( results )
			.map({ key : 0, value : 1 })
			.filter( 'key', notIn( 'kind', 'etag' ) )
			.filter( typeOf( 'value' ), notIn( 'object', 'function' ) )
			.map( toString( 'key', ': ', 'value' ) )
			.forEach( Logger.log )
			*/
		}while( pageToken );
		
		return results.schema ? rows.map( stripUselessBoilerplate ).map( parseRow( results.schema.fields ) ) : rows;
	}
}

function waitForJobToFinish({
		projectId = BQ.PROJECT_ID,
		jobId,
		seconds = 2048
	}){
	if( typeof arguments[ 0 ] == 'string' ){
		jobId   = arguments[ 0 ];
		seconds = arguments[ 1 ] || seconds;
	}
	jobId = sanitizeJobId( jobId );
	
	const getState = () => ( BigQuery.Jobs.get( projectId, jobId ).status || {} ).state;
	let waited = 0;
	for(
			let i = 0,
			state = getState()
			;
			state != 'DONE' && Math.pow( 2, i + 1 ) - 1 <= seconds
			;
			i++,
			state = getState()
		){
			const secondsToWait = Math.pow( 2, i );
			Logger.log(
				`Status is "${state}". Waiting for the job to finish. Sleep ${secondsToWait} seconds.`
			);
			Utilities.sleep( 1000 * secondsToWait );
			waited += secondsToWait;
	}
	const job = BigQuery.Jobs.get( projectId, jobId );
	const status = job.status;
	if( status && status.errors && status.errors.length > 0 ){
		Logger.log( 'job-errors: ' + JSON.stringify( status.errors, null, 2 ) );
	}
	let result = {
		job                   : job,
		isDone                : ( status || {} ).state === 'DONE',
		state                 : ( status || {} ).state,
		hasErrors             : status && status.errors && status.errors.length > 0,
		errors                : ( status || {} ).errors || [],
		throwIfNotSuccsessful : message => {
			if( ( status || {} ).state != 'DONE' ){
				throw new Error( message || 'Bigquery job still not finished after ' + waited + ' seconds.' );
			}
			if( status && status.errors && status.errors.length > 0 ){
				throw new Error( message || 'Bigquery job failed: ' + status.errors );
			}
		},
		print                  : _ => {
			Logger.log( result.state + ' ' + result.errors );
		},
	};
	return result;
}

function queryTableOrView({ reference, query, onlyCurrentPartition, projectId }){
	if( !query ){
		query = `SELECT * FROM ${reference.projectId}.${reference.datasetId}.${reference.tableId} WHERE TRUE ${ onlyCurrentPartition ? 'AND _PARTITIONDATE = CURRENT_DATE()' : '' }`;
	}
	const job = BigQuery.Jobs.insert(
		{
			configuration: {
				query: {
					query        : query,
					useLegacySql : false, // this is still necessary
				}
			}
		},
		projectId || reference.projectId
	);
	const jobId = sanitizeJobId( job.id );
	waitForJobToFinish( jobId );
	
	return bq.getQueryResults( { projectId : projectId || reference.projectId, jobId : jobId } );
}

function sanitizeJobId( jobId ){
	if( jobId.includes( '.' ) ){
		// Invalid job ID "biddy-io:US.job_nLbcaw3YDYShaqKhy-QYbVbYDyQs"
		// Need to remove "biddy-io:US." part
		return jobId.substring( jobId.lastIndexOf( '.' ) + 1 );
	}
	return jobId;
}

function queryBigqueryAsync({ query, reference, writeDisposition = 'WRITE_APPEND', createDisposition = 'CREATE_IF_NEEDED' }){
	const job = BigQuery.Jobs.insert(
		{
			configuration: {
				query: {
					destinationTable  : reference,
					query             : query,
					useLegacySql      : false,             // this is still necessary
					createDisposition : createDisposition, // [ CREATE_IF_NEEDED, CREATE_NEVER ]
					writeDisposition  : writeDisposition,  // [ WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY ]
				}
			}
		},
		reference.projectId
	);
	const jobId = sanitizeJobId( job.id );
	return waitForJobToFinish( jobId );
}

// ----- BigQuery -- END ---------


