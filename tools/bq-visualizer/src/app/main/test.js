module.exports = {
  "kind": "bigquery#job",
  "etag": "7F4lktJiSy+lv62IHKgdbA==",
  "id": "som-phs-redivis-prod:US.111c86ff-147a-4c76-b82d-d510e4024cfb",
  "selfLink": "https://www.googleapis.com/bigquery/v2/projects/som-phs-redivis-prod/jobs/111c86ff-147a-4c76-b82d-d510e4024cfb?location=US",
  "user_email": "936051303296-compute@developer.gserviceaccount.com",
  "configuration": {
    "query": {
      "query": "/*\n## Create date: 3/30/20\n## Last Modified: 4/10/20\n## Transform 44\n\nOutput\n\tOne row per (family_id, synthetic_emerg_date, synthetic_emerg_number)-pair.\n\t10 rows per family_id each with randomly drawn synthetic emergency date, where the synthetic \n\temergency date is drawn (with replacement) from the (possible duplicate) list of actual \n\temergency dates falling within the dates of coverage for the family_id.\n\nExclusion criteria\n\t1. Exclude families with patid that are ever in the treated group\n\t2. Must have no ER nor inpatient hospitalization\n\t\tWHERE Tos_Cd NOT IN (\"FAC_OP.ER\", \"PROF.ER\") AND Conf_Id IS Null \n\twithin one year prior *nor* one year post synthetic emergency date\n\t3. Must have no pregnancy related claims\n\t\t(SAFE_CAST(claims.Proc_Cd AS INT64) \u003e= 59000 OR SAFE_CAST(claims.Proc_Cd AS INT64)\u003c=59899)\n\tbetween the day of and up to one year prior to the post synthetic emergency date\n\t4. (possible) Must have no pregnancy related claims\n\t\t(SAFE_CAST(Proc_Cd AS INT64) \u003e= 59000 OR SAFE_CAST(Proc_Cd AS INT64)\u003c=59899)\n\tbetween the day following through one year after the post synthetic emergency date\n\nNote\n\tFamilies failing exclusion criteria #1 are NOT included in the output\n\tFamily-synthetic emergency dates failing exclusion criteria #2-#4 ARE included in the output, \n\twith a clear indication (=1) of which criteria it failed.\n\n*/\n\nWITH\n\n/* All emergency dates and family_ids */\nemergency_families AS (\n\tSELECT DISTINCT\n\t\tfirst_emerg_date, CAST(family_id AS INT64) AS family_id\n\tFROM (\n\t\tSELECT \n\t\t\tfirst_emerg_date,\n\t\t\tSPLIT(all_Family_Id, \",\") AS family_ids\n\t\tFROM \n\t\t\t-- Tranform 9\n\t\t\tproject_2907.66231\n\t),\n\tUNNEST(family_ids) AS family_id\n),\n\n/* Obtain candidate control families */\n## Moved to fam_claims transform on a 100% data pull.\n-- fam_claims AS (\n-- \tSELECT DISTINCT\n-- \t\tfamily_id, fst_dt,\n-- \t\tEligeff, Eligend,\n-- \t\tIF((Admit_Type =1 OR Admit_Type=2) AND (Tos_Cd IN (\"FAC_OP.ER\", \"PROF.ER\", \"FAC_IP.ACUTE\", \"PROF.INPVIS\") OR Conf_Id IS Null), 1, 0) AS er_inpatient_hospitalization,\n--       \t-- But not a pregnancy related emergency\n-- \t\tIF((SAFE_CAST(Proc_Cd AS INT64) \u003e= 59000 AND SAFE_CAST(Proc_Cd AS INT64)\u003c=59899), 1, 0) AS pregnancy\n-- \tFROM\n-- \t\t(SELECT patid, pat_planid, Fst_dt, Proc_Cd, Clmid, Admit_Type, Tos_Cd, conf_id FROM stanfordphs.optum_dod:138:v3_0.medical_claims:9)\n-- \tINNER JOIN\n-- \t\t(SELECT patid, pat_planid, family_id, Eligeff, Eligend, yrdob FROM stanfordphs.optum_dod:138:v3_0.member_enrollment:3)\n-- \tUSING(patid, Pat_Planid)\n-- \tWHERE\n-- \t\t(Fst_Dt BETWEEN Eligeff AND Eligend)\n-- ),\n\nrandom_sample AS (\nSELECT DISTINCT family_id\nFROM project_2907.67067\nWHERE ABS(MOD(FARM_FINGERPRINT(CAST(family_id AS STRING)), 10)) = 9\n## WHERE RAND()\u003c=0.1\n),\n\nuse_fam_claims1 AS (\n\tSELECT *\n\tFROM (\n\t\tSELECT DISTINCT family_id\n\t\tFROM project_2907.67067\n\t\tEXCEPT DISTINCT\n\t\tSELECT family_id\n\t\tFROM emergency_families\n\t)\n\tINNER JOIN random_sample\n\tUSING(family_id)\n),\n######################################\n##### Added this on 4/8/20 ###########\nuse_fam_claims AS (\nSELECT * \nFROM project_2907.67067\nINNER JOIN use_fam_claims1\nUSING(family_id)\n),\n######################################\n\n\n/* Obtain continuous enrollment dates for each family */\nfam_continuous_enrollment AS (\nSELECT\n\tfamily_id,\n\tARRAY_AGG(DISTINCT coverage_date) AS coverage_dates\nFROM (\n\tSELECT\n\t\tfamily_id, \n\t\tGENERATE_DATE_ARRAY(eligeff, eligend) AS coverage_dates\n\tFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\n\t),\n\tUNNEST(coverage_dates) AS coverage_date\nGROUP BY family_id\n),\n\n/* Join all emergency dates unto the non-emergency families */\ntable_w_all_emerg_dates_unto_non_emergency_families AS (\n\n/* \"Draw\" number 1 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 2 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 3 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 4 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 5 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 6 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 7 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 8 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 9 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\nUNION ALL\n/* \"Draw\" number 10 */\nSELECT family_id, first_emerg_date AS synth_emerg_date, RAND() AS to_sort_order\nFROM (SELECT DISTINCT family_id, eligeff, eligend FROM use_fam_claims)\nCROSS JOIN (SELECT first_emerg_date FROM emergency_families)\nWHERE first_emerg_date BETWEEN eligeff AND eligend\n)\n\n/* Final table */\nSELECT DISTINCT\n\tfamily_id, synth_emerg_date, synth_emerg_number,\n\t/* Column for exclusion criteria #2 */\n\tMAX(IF(\n\t\ter_inpatient_hospitalization = 1 AND\n\t\tfst_dt BETWEEN \n\t\tDATE_SUB(synth_emerg_date, INTERVAL 365 DAY) \n\t\tAND DATE_ADD(synth_emerg_date, INTERVAL 365 DAY),\n\t\t1, 0\n\t)) AS er_inpatient_hospitalization_prior_post_syn_emerg,\n\t/* Column for exclusion criteria #3 */\n\tMAX(IF(\n\t\tpregnancy = 1 AND\n\t\tfst_dt BETWEEN \n\t\tDATE_SUB(synth_emerg_date, INTERVAL 365 DAY) \n\t\tAND synth_emerg_date,\n\t\t1, 0\n\t)) AS pregnancy_on_or_one_year_before_syn_emerg,\n\t/* Column for exclusion criteria #4 */\n\tMAX(IF(\n\t\tpregnancy = 1 AND\n\t\tfst_dt BETWEEN \n\t\tDATE_ADD(synth_emerg_date, INTERVAL 1 DAY)\n\t\tAND DATE_ADD(synth_emerg_date, INTERVAL 365 DAY),\n\t\t1, 0\n\t)) AS pregnancy_one_day_to_one_year_after_syn_emerg\nFROM (\n\tSELECT\n\t\tfamily_id,\n\t\tsynth_emerg_date,\n\t\tROW_NUMBER() OVER(PARTITION BY family_id ORDER BY to_sort_order) AS synth_emerg_number\n\tFROM table_w_all_emerg_dates_unto_non_emergency_families\n\t)\nLEFT JOIN \n\t(SELECT DISTINCT family_id, fst_dt, pregnancy, er_inpatient_hospitalization FROM use_fam_claims)\nUSING(family_id)\nWHERE \n\tsynth_emerg_number \u003c= 10\nGROUP BY family_id, synth_emerg_date, synth_emerg_number\n\n",
      "destinationTable": {
        "projectId": "som-phs-redivis-prod",
        "datasetId": "project_2907",
        "tableId": "67029"
      },
      "createDisposition": "CREATE_IF_NEEDED",
      "writeDisposition": "WRITE_TRUNCATE",
      "defaultDataset": {
        "datasetId": "project_2907",
        "projectId": "som-phs-redivis-prod"
      },
      "priority": "INTERACTIVE",
      "useQueryCache": true,
      "maximumBytesBilled": "1000000000000",
      "useLegacySql": false,
      "parameterMode": "NAMED"
    },
    "jobType": "QUERY"
  },
  "jobReference": {
    "projectId": "som-phs-redivis-prod",
    "jobId": "111c86ff-147a-4c76-b82d-d510e4024cfb",
    "location": "US"
  },
  "statistics": {
    "creationTime": "1586539841602",
    "startTime": "1586539841919",
    "endTime": "1586539969319",
    "query": {
      "queryPlan": [
        {
          "name": "S00: Input",
          "id": "0",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "184",
          "readMsMax": "209",
          "computeMsAvg": "1446",
          "computeMsMax": "2893",
          "writeMsAvg": "49",
          "writeMsMax": "55",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$530:first_emerg_date, $531:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2870 := $530, $2871 := $1110"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1110 := CAST($1310 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1300 := split($531, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2870, $2871",
                "TO __stage00_output",
                "BY HASH($2870, $2871)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S01: Input",
          "id": "1",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "140",
          "readMsMax": "173",
          "computeMsAvg": "1427",
          "computeMsMax": "2855",
          "writeMsAvg": "52",
          "writeMsMax": "62",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$520:first_emerg_date, $521:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2700 := $520, $2701 := $1100"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1100 := CAST($1290 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1280 := split($521, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2700, $2701",
                "TO __stage01_output",
                "BY HASH($2700, $2701)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S02: Input",
          "id": "2",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "44",
          "readMsMax": "63",
          "computeMsAvg": "1378",
          "computeMsMax": "2757",
          "writeMsAvg": "44",
          "writeMsMax": "49",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$510:first_emerg_date, $511:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2530 := $510, $2531 := $1090"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1090 := CAST($1270 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1260 := split($511, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2530, $2531",
                "TO __stage02_output",
                "BY HASH($2530, $2531)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S03: Input",
          "id": "3",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "40",
          "readMsMax": "60",
          "computeMsAvg": "1335",
          "computeMsMax": "2671",
          "writeMsAvg": "51",
          "writeMsMax": "58",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$500:first_emerg_date, $501:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2360 := $500, $2361 := $1080"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1080 := CAST($1250 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1240 := split($501, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2360, $2361",
                "TO __stage03_output",
                "BY HASH($2360, $2361)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S04: Input",
          "id": "4",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "32",
          "readMsMax": "35",
          "computeMsAvg": "1586",
          "computeMsMax": "3172",
          "writeMsAvg": "48",
          "writeMsMax": "55",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$490:first_emerg_date, $491:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2190 := $490, $2191 := $1070"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1070 := CAST($1230 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1220 := split($491, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2190, $2191",
                "TO __stage04_output",
                "BY HASH($2190, $2191)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S05: Input",
          "id": "5",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "26",
          "readMsMax": "28",
          "computeMsAvg": "1566",
          "computeMsMax": "3133",
          "writeMsAvg": "73",
          "writeMsMax": "82",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$480:first_emerg_date, $481:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2020 := $480, $2021 := $1060"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1060 := CAST($1210 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1200 := split($481, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2020, $2021",
                "TO __stage05_output",
                "BY HASH($2020, $2021)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S06: Input",
          "id": "6",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "31",
          "readMsMax": "35",
          "computeMsAvg": "1403",
          "computeMsMax": "2807",
          "writeMsAvg": "61",
          "writeMsMax": "89",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$470:first_emerg_date, $471:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1850 := $470, $1851 := $1050"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1050 := CAST($1190 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1180 := split($471, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1850, $1851",
                "TO __stage06_output",
                "BY HASH($1850, $1851)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S07: Input",
          "id": "7",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "26",
          "readMsMax": "27",
          "computeMsAvg": "1269",
          "computeMsMax": "2539",
          "writeMsAvg": "58",
          "writeMsMax": "74",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$460:first_emerg_date, $461:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1680 := $460, $1681 := $1040"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1040 := CAST($1170 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1160 := split($461, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1680, $1681",
                "TO __stage07_output",
                "BY HASH($1680, $1681)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S08: Input",
          "id": "8",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "32",
          "readMsMax": "35",
          "computeMsAvg": "1465",
          "computeMsMax": "2931",
          "writeMsAvg": "46",
          "writeMsMax": "61",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$450:first_emerg_date, $451:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1510 := $450, $1511 := $1030"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1030 := CAST($1150 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1140 := split($451, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1510, $1511",
                "TO __stage08_output",
                "BY HASH($1510, $1511)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S09: Input",
          "id": "9",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "34",
          "readMsMax": "39",
          "computeMsAvg": "1457",
          "computeMsMax": "2914",
          "writeMsAvg": "39",
          "writeMsMax": "44",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$440:first_emerg_date, $441:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1340 := $440, $1341 := $1020"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1020 := CAST($1130 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1120 := split($441, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1340, $1341",
                "TO __stage09_output",
                "BY HASH($1340, $1341)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S0A: Input",
          "id": "10",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "140",
          "readMsMax": "440",
          "computeMsAvg": "14289",
          "computeMsMax": "2872206",
          "writeMsAvg": "570",
          "writeMsMax": "1038",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$430:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2850 := $430"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2850",
                "TO __stage0A_output",
                "BY HASH($2850)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S0B: Input",
          "id": "11",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "54",
          "readMsMax": "314",
          "computeMsAvg": "14398",
          "computeMsMax": "2894128",
          "writeMsAvg": "586",
          "writeMsMax": "1783",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$420:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2680 := $420"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2680",
                "TO __stage0B_output",
                "BY HASH($2680)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S0C: Input",
          "id": "12",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "51",
          "readMsMax": "217",
          "computeMsAvg": "14379",
          "computeMsMax": "2890298",
          "writeMsAvg": "590",
          "writeMsMax": "1364",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$410:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2510 := $410"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2510",
                "TO __stage0C_output",
                "BY HASH($2510)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S0D: Input",
          "id": "13",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "51",
          "readMsMax": "189",
          "computeMsAvg": "14394",
          "computeMsMax": "2893289",
          "writeMsAvg": "568",
          "writeMsMax": "1248",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$400:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2340 := $400"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2340",
                "TO __stage0D_output",
                "BY HASH($2340)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S0E: Input",
          "id": "14",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "52",
          "readMsMax": "210",
          "computeMsAvg": "14427",
          "computeMsMax": "2899909",
          "writeMsAvg": "579",
          "writeMsMax": "1061",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$390:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2170 := $390"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2170",
                "TO __stage0E_output",
                "BY HASH($2170)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S0F: Input",
          "id": "15",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "51",
          "readMsMax": "347",
          "computeMsAvg": "14375",
          "computeMsMax": "2889498",
          "writeMsAvg": "576",
          "writeMsMax": "1295",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$380:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2000 := $380"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2000",
                "TO __stage0F_output",
                "BY HASH($2000)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S10: Input",
          "id": "16",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "56",
          "readMsMax": "414",
          "computeMsAvg": "14155",
          "computeMsMax": "2845165",
          "writeMsAvg": "562",
          "writeMsMax": "1005",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$370:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1830 := $370"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1830",
                "TO __stage10_output",
                "BY HASH($1830)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S11: Input",
          "id": "17",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "60",
          "readMsMax": "497",
          "computeMsAvg": "14416",
          "computeMsMax": "2897637",
          "writeMsAvg": "571",
          "writeMsMax": "1192",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$360:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1660 := $360"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1660",
                "TO __stage11_output",
                "BY HASH($1660)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S12: Input",
          "id": "18",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "58",
          "readMsMax": "262",
          "computeMsAvg": "14384",
          "computeMsMax": "2891376",
          "writeMsAvg": "571",
          "writeMsMax": "2288",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$350:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1490 := $350"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1490",
                "TO __stage12_output",
                "BY HASH($1490)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S13: Input",
          "id": "19",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "60",
          "readMsMax": "226",
          "computeMsAvg": "14472",
          "computeMsMax": "2908893",
          "writeMsAvg": "558",
          "writeMsMax": "954",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$340:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1320 := $340"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1320",
                "TO __stage13_output",
                "BY HASH($1320)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S14: Input",
          "id": "20",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "64",
          "readMsMax": "262",
          "computeMsAvg": "4116",
          "computeMsMax": "827358",
          "writeMsAvg": "74",
          "writeMsMax": "193",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$330:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($330 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2930 := $330"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2930",
                "TO __stage14_output",
                "BY HASH($2930)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S15: Input",
          "id": "21",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "71",
          "readMsMax": "402",
          "computeMsAvg": "4084",
          "computeMsMax": "821062",
          "writeMsAvg": "85",
          "writeMsMax": "311",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$320:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($320 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2760 := $320"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2760",
                "TO __stage15_output",
                "BY HASH($2760)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S16: Input",
          "id": "22",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "69",
          "readMsMax": "310",
          "computeMsAvg": "4099",
          "computeMsMax": "823966",
          "writeMsAvg": "75",
          "writeMsMax": "217",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$310:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($310 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2590 := $310"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2590",
                "TO __stage16_output",
                "BY HASH($2590)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S17: Input",
          "id": "23",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "65",
          "readMsMax": "221",
          "computeMsAvg": "4053",
          "computeMsMax": "814736",
          "writeMsAvg": "68",
          "writeMsMax": "227",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$300:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($300 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2420 := $300"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2420",
                "TO __stage17_output",
                "BY HASH($2420)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S18: Input",
          "id": "24",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "68",
          "readMsMax": "260",
          "computeMsAvg": "4092",
          "computeMsMax": "822663",
          "writeMsAvg": "71",
          "writeMsMax": "355",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$290:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($290 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2250 := $290"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2250",
                "TO __stage18_output",
                "BY HASH($2250)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S19: Input",
          "id": "25",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "66",
          "readMsMax": "256",
          "computeMsAvg": "4096",
          "computeMsMax": "823360",
          "writeMsAvg": "78",
          "writeMsMax": "188",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$280:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($280 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2080 := $280"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2080",
                "TO __stage19_output",
                "BY HASH($2080)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S1A: Input",
          "id": "26",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "67",
          "readMsMax": "307",
          "computeMsAvg": "4113",
          "computeMsMax": "826862",
          "writeMsAvg": "75",
          "writeMsMax": "290",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$270:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($270 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1910 := $270"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1910",
                "TO __stage1A_output",
                "BY HASH($1910)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S1B: Input",
          "id": "27",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "71",
          "readMsMax": "290",
          "computeMsAvg": "4092",
          "computeMsMax": "822639",
          "writeMsAvg": "68",
          "writeMsMax": "180",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$260:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($260 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1740 := $260"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1740",
                "TO __stage1B_output",
                "BY HASH($1740)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S1C: Input",
          "id": "28",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "69",
          "readMsMax": "216",
          "computeMsAvg": "4105",
          "computeMsMax": "825122",
          "writeMsAvg": "68",
          "writeMsMax": "154",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$250:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($250 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1570 := $250"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1570",
                "TO __stage1C_output",
                "BY HASH($1570)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S1D: Input",
          "id": "29",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "73",
          "readMsMax": "382",
          "computeMsAvg": "4094",
          "computeMsMax": "822922",
          "writeMsAvg": "68",
          "writeMsMax": "232",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$240:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($240 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1400 := $240"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1400",
                "TO __stage1D_output",
                "BY HASH($1400)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S1E: Input",
          "id": "30",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "42",
          "readMsMax": "62",
          "computeMsAvg": "1444",
          "computeMsMax": "2889",
          "writeMsAvg": "47",
          "writeMsMax": "63",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$230:first_emerg_date, $231:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3060 := $230, $3061 := $990"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$990 := CAST($1010 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$1000 := split($231, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3060, $3061",
                "TO __stage1E_output",
                "BY HASH($3060, $3061)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S1F: Input",
          "id": "31",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "59",
          "readMsMax": "179",
          "computeMsAvg": "14427",
          "computeMsMax": "2899904",
          "writeMsAvg": "580",
          "writeMsMax": "1349",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$120:family_id",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3040 := $120"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3040",
                "TO __stage1F_output",
                "BY HASH($3040)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S20: Input",
          "id": "32",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "48",
          "readMsMax": "52",
          "computeMsAvg": "1546",
          "computeMsMax": "3093",
          "writeMsAvg": "67",
          "writeMsMax": "98",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$220:first_emerg_date, $221:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2990 := $220, $2991 := $780"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$780 := CAST($980 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$970 := split($221, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2990, $2991",
                "TO __stage20_output",
                "BY HASH($2990, $2991)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S21: Input",
          "id": "33",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "26",
          "readMsMax": "28",
          "computeMsAvg": "1377",
          "computeMsMax": "2754",
          "writeMsAvg": "49",
          "writeMsMax": "57",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$210:first_emerg_date, $211:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2820 := $210, $2821 := $770"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$770 := CAST($960 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$950 := split($211, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2820, $2821",
                "TO __stage21_output",
                "BY HASH($2820, $2821)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S22: Input",
          "id": "34",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "27",
          "readMsMax": "28",
          "computeMsAvg": "1385",
          "computeMsMax": "2771",
          "writeMsAvg": "40",
          "writeMsMax": "50",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$200:first_emerg_date, $201:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2650 := $200, $2651 := $760"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$760 := CAST($940 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$930 := split($201, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2650, $2651",
                "TO __stage22_output",
                "BY HASH($2650, $2651)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S23: Input",
          "id": "35",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "29",
          "readMsMax": "33",
          "computeMsAvg": "1458",
          "computeMsMax": "2917",
          "writeMsAvg": "46",
          "writeMsMax": "56",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$190:first_emerg_date, $191:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2480 := $190, $2481 := $750"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$750 := CAST($920 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$910 := split($191, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2480, $2481",
                "TO __stage23_output",
                "BY HASH($2480, $2481)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S24: Input",
          "id": "36",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "41",
          "readMsMax": "50",
          "computeMsAvg": "1624",
          "computeMsMax": "3249",
          "writeMsAvg": "65",
          "writeMsMax": "69",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$180:first_emerg_date, $181:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2310 := $180, $2311 := $740"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$740 := CAST($900 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$890 := split($181, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2310, $2311",
                "TO __stage24_output",
                "BY HASH($2310, $2311)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S25: Input",
          "id": "37",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "45",
          "readMsMax": "59",
          "computeMsAvg": "1683",
          "computeMsMax": "3366",
          "writeMsAvg": "71",
          "writeMsMax": "101",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$170:first_emerg_date, $171:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2140 := $170, $2141 := $730"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$730 := CAST($880 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$870 := split($171, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2140, $2141",
                "TO __stage25_output",
                "BY HASH($2140, $2141)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S26: Input",
          "id": "38",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "23",
          "readMsMax": "25",
          "computeMsAvg": "1370",
          "computeMsMax": "2741",
          "writeMsAvg": "67",
          "writeMsMax": "88",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$160:first_emerg_date, $161:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1970 := $160, $1971 := $720"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$720 := CAST($860 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$850 := split($161, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1970, $1971",
                "TO __stage26_output",
                "BY HASH($1970, $1971)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S27: Input",
          "id": "39",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "38",
          "readMsMax": "39",
          "computeMsAvg": "1411",
          "computeMsMax": "2822",
          "writeMsAvg": "42",
          "writeMsMax": "47",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$150:first_emerg_date, $151:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1800 := $150, $1801 := $710"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$710 := CAST($840 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$830 := split($151, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1800, $1801",
                "TO __stage27_output",
                "BY HASH($1800, $1801)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S28: Input",
          "id": "40",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "29",
          "readMsMax": "30",
          "computeMsAvg": "1504",
          "computeMsMax": "3008",
          "writeMsAvg": "117",
          "writeMsMax": "166",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$140:first_emerg_date, $141:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1630 := $140, $1631 := $700"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$700 := CAST($820 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$810 := split($141, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1630, $1631",
                "TO __stage28_output",
                "BY HASH($1630, $1631)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S29: Input",
          "id": "41",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "28",
          "readMsMax": "35",
          "computeMsAvg": "1398",
          "computeMsMax": "2797",
          "writeMsAvg": "51",
          "writeMsMax": "56",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$130:first_emerg_date, $131:all_Family_Id",
                "FROM project_2907.66231"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1460 := $130, $1461 := $690"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$690 := CAST($800 AS INT64)"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$790 := split($131, ',')"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1460, $1461",
                "TO __stage29_output",
                "BY HASH($1460, $1461)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S2A: Input",
          "id": "42",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "85",
          "readMsMax": "363",
          "computeMsAvg": "4046",
          "computeMsMax": "813327",
          "writeMsAvg": "79",
          "writeMsMax": "260",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$10:family_id",
                "FROM project_2907.67067",
                "WHERE equal(abs(mod(farm_fingerprint(CAST($10 AS STRING)), 10)), 9)"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3120 := $10"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3120",
                "TO __stage2A_output",
                "BY HASH($3120)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S2B: Repartition",
          "id": "43",
          "inputStages": [
            "10"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5549",
          "computeMsMax": "33294",
          "writeMsAvg": "608",
          "writeMsMax": "724",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage0A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S2C: Repartition",
          "id": "44",
          "inputStages": [
            "43",
            "10"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6531",
          "computeMsMax": "84910",
          "writeMsAvg": "672",
          "writeMsMax": "1215",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "13",
          "completedParallelInputs": "13",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage2B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S2D: Repartition",
          "id": "45",
          "inputStages": [
            "44",
            "10"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6073",
          "computeMsMax": "6073",
          "writeMsAvg": "883",
          "writeMsMax": "883",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage2C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S2E: Repartition",
          "id": "46",
          "inputStages": [
            "11"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5123",
          "computeMsMax": "30742",
          "writeMsAvg": "563",
          "writeMsMax": "679",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage0B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S2F: Repartition",
          "id": "47",
          "inputStages": [
            "46",
            "11"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6108",
          "computeMsMax": "36651",
          "writeMsAvg": "524",
          "writeMsMax": "629",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage2E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S30: Repartition",
          "id": "48",
          "inputStages": [
            "12"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4860",
          "computeMsMax": "29165",
          "writeMsAvg": "476",
          "writeMsMax": "575",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage0C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S31: Repartition",
          "id": "49",
          "inputStages": [
            "47",
            "11"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6771",
          "computeMsMax": "54170",
          "writeMsAvg": "696",
          "writeMsMax": "864",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "8",
          "completedParallelInputs": "8",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage2F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S32: Repartition",
          "id": "50",
          "inputStages": [
            "48",
            "12"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5490",
          "computeMsMax": "32945",
          "writeMsAvg": "544",
          "writeMsMax": "607",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage30_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S33: Repartition",
          "id": "51",
          "inputStages": [
            "13"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4725",
          "computeMsMax": "28354",
          "writeMsAvg": "465",
          "writeMsMax": "541",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage0D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S34: Repartition",
          "id": "52",
          "inputStages": [
            "50",
            "12"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6219",
          "computeMsMax": "37317",
          "writeMsAvg": "574",
          "writeMsMax": "665",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage32_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S35: Repartition",
          "id": "53",
          "inputStages": [
            "51",
            "13"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5952",
          "computeMsMax": "41670",
          "writeMsAvg": "729",
          "writeMsMax": "997",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage33_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S36: Repartition",
          "id": "54",
          "inputStages": [
            "52",
            "12"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7579",
          "computeMsMax": "15159",
          "writeMsAvg": "816",
          "writeMsMax": "838",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage34_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S37: Repartition",
          "id": "55",
          "inputStages": [
            "53",
            "13"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6429",
          "computeMsMax": "45009",
          "writeMsAvg": "638",
          "writeMsMax": "854",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage35_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S38: Repartition",
          "id": "56",
          "inputStages": [
            "14"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5459",
          "computeMsMax": "60057",
          "writeMsAvg": "678",
          "writeMsMax": "1009",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "11",
          "completedParallelInputs": "11",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage0E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S39: Repartition",
          "id": "57",
          "inputStages": [
            "15"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5324",
          "computeMsMax": "42593",
          "writeMsAvg": "500",
          "writeMsMax": "568",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "8",
          "completedParallelInputs": "8",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage0F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S3A: Repartition",
          "id": "58",
          "inputStages": [
            "31"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5032",
          "computeMsMax": "30196",
          "writeMsAvg": "493",
          "writeMsMax": "567",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage1F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S3B: Repartition",
          "id": "59",
          "inputStages": [
            "57",
            "15"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5451",
          "computeMsMax": "32706",
          "writeMsAvg": "570",
          "writeMsMax": "644",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage39_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S3C: Repartition",
          "id": "60",
          "inputStages": [
            "16"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5214",
          "computeMsMax": "41714",
          "writeMsAvg": "527",
          "writeMsMax": "781",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "8",
          "completedParallelInputs": "8",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage10_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S3D: Repartition",
          "id": "61",
          "inputStages": [
            "58",
            "31"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5213",
          "computeMsMax": "31281",
          "writeMsAvg": "717",
          "writeMsMax": "901",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage3A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S3E: Repartition",
          "id": "62",
          "inputStages": [
            "56",
            "14"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6981",
          "computeMsMax": "55851",
          "writeMsAvg": "721",
          "writeMsMax": "970",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "8",
          "completedParallelInputs": "8",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage38_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S3F: Repartition",
          "id": "63",
          "inputStages": [
            "62",
            "14"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7475",
          "computeMsMax": "7475",
          "writeMsAvg": "590",
          "writeMsMax": "590",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage3E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S40: Repartition",
          "id": "64",
          "inputStages": [
            "18"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5117",
          "computeMsMax": "30707",
          "writeMsAvg": "506",
          "writeMsMax": "591",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage12_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S41: Repartition",
          "id": "65",
          "inputStages": [
            "60",
            "16"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5892",
          "computeMsMax": "35353",
          "writeMsAvg": "625",
          "writeMsMax": "812",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage3C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S42: Repartition",
          "id": "66",
          "inputStages": [
            "59",
            "15"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6379",
          "computeMsMax": "38276",
          "writeMsAvg": "633",
          "writeMsMax": "870",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage3B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S43: Repartition",
          "id": "67",
          "inputStages": [
            "61",
            "31"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6314",
          "computeMsMax": "44199",
          "writeMsAvg": "650",
          "writeMsMax": "848",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage3D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S44: Repartition",
          "id": "68",
          "inputStages": [
            "64",
            "18"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5920",
          "computeMsMax": "35521",
          "writeMsAvg": "657",
          "writeMsMax": "824",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage40_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S45: Repartition",
          "id": "69",
          "inputStages": [
            "65",
            "16"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7148",
          "computeMsMax": "42890",
          "writeMsAvg": "661",
          "writeMsMax": "795",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage41_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S46: Repartition",
          "id": "70",
          "inputStages": [
            "67",
            "31"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4550",
          "computeMsMax": "4550",
          "writeMsAvg": "506",
          "writeMsMax": "506",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage43_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S47: Repartition",
          "id": "71",
          "inputStages": [
            "42"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4530",
          "computeMsMax": "9061",
          "writeMsAvg": "397",
          "writeMsMax": "419",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage2A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S48: Repartition",
          "id": "72",
          "inputStages": [
            "17"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5845",
          "computeMsMax": "87682",
          "writeMsAvg": "557",
          "writeMsMax": "931",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "15",
          "completedParallelInputs": "15",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage11_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S49: Repartition",
          "id": "73",
          "inputStages": [
            "68",
            "18"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6594",
          "computeMsMax": "46162",
          "writeMsAvg": "600",
          "writeMsMax": "792",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage44_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S4A: Repartition",
          "id": "74",
          "inputStages": [
            "19"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6026",
          "computeMsMax": "42183",
          "writeMsAvg": "549",
          "writeMsMax": "625",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage13_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S4B: Repartition",
          "id": "75",
          "inputStages": [
            "73",
            "18"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7059",
          "computeMsMax": "7059",
          "writeMsAvg": "1257",
          "writeMsMax": "1257",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage49_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S4C: Repartition",
          "id": "76",
          "inputStages": [
            "72",
            "17"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5531",
          "computeMsMax": "27657",
          "writeMsAvg": "582",
          "writeMsMax": "631",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "5",
          "completedParallelInputs": "5",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage48_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S4D: Repartition",
          "id": "77",
          "inputStages": [
            "74",
            "19"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6427",
          "computeMsMax": "44992",
          "writeMsAvg": "625",
          "writeMsMax": "734",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage4A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S4E: Repartition",
          "id": "78",
          "inputStages": [
            "77",
            "19"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7303",
          "computeMsMax": "43821",
          "writeMsAvg": "704",
          "writeMsMax": "886",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage4D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S4F: Repartition",
          "id": "79",
          "inputStages": [
            "20"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4674",
          "computeMsMax": "14024",
          "writeMsAvg": "445",
          "writeMsMax": "541",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "3",
          "completedParallelInputs": "3",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage14_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S50: Repartition",
          "id": "80",
          "inputStages": [
            "25"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4598",
          "computeMsMax": "4598",
          "writeMsAvg": "455",
          "writeMsMax": "455",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage19_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S51: Repartition",
          "id": "81",
          "inputStages": [
            "28"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5334",
          "computeMsMax": "10669",
          "writeMsAvg": "566",
          "writeMsMax": "615",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage1C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S52: Repartition",
          "id": "82",
          "inputStages": [
            "22"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4824",
          "computeMsMax": "9649",
          "writeMsAvg": "429",
          "writeMsMax": "447",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage16_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S53: Repartition",
          "id": "83",
          "inputStages": [
            "29"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4044",
          "computeMsMax": "12134",
          "writeMsAvg": "376",
          "writeMsMax": "409",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "3",
          "completedParallelInputs": "3",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage1D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S54: Repartition",
          "id": "84",
          "inputStages": [
            "26"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4426",
          "computeMsMax": "13279",
          "writeMsAvg": "444",
          "writeMsMax": "531",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "3",
          "completedParallelInputs": "3",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage1A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S55: Repartition",
          "id": "85",
          "inputStages": [
            "27"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4552",
          "computeMsMax": "22760",
          "writeMsAvg": "440",
          "writeMsMax": "547",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "5",
          "completedParallelInputs": "5",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage1B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S56: Repartition",
          "id": "86",
          "inputStages": [
            "21"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4745",
          "computeMsMax": "4745",
          "writeMsAvg": "426",
          "writeMsMax": "426",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage15_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S57: Repartition",
          "id": "87",
          "inputStages": [
            "49",
            "11"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5401",
          "computeMsMax": "307871",
          "writeMsAvg": "688",
          "writeMsMax": "919",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "57",
          "completedParallelInputs": "57",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage31_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S58: Repartition",
          "id": "88",
          "inputStages": [
            "54",
            "12"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5177",
          "computeMsMax": "289926",
          "writeMsAvg": "672",
          "writeMsMax": "1191",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "56",
          "completedParallelInputs": "56",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage36_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S59: Repartition",
          "id": "89",
          "inputStages": [
            "70",
            "31"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5552",
          "computeMsMax": "399795",
          "writeMsAvg": "739",
          "writeMsMax": "1174",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "72",
          "completedParallelInputs": "72",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage46_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S5A: Repartition",
          "id": "90",
          "inputStages": [
            "63",
            "14"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5502",
          "computeMsMax": "396179",
          "writeMsAvg": "757",
          "writeMsMax": "1122",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "72",
          "completedParallelInputs": "72",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage3F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S5B: Repartition",
          "id": "91",
          "inputStages": [
            "69",
            "16"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5651",
          "computeMsMax": "480388",
          "writeMsAvg": "732",
          "writeMsMax": "924",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "85",
          "completedParallelInputs": "85",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage45_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S5C: Repartition",
          "id": "92",
          "inputStages": [
            "55",
            "13"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5504",
          "computeMsMax": "418377",
          "writeMsAvg": "704",
          "writeMsMax": "877",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "76",
          "completedParallelInputs": "76",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage37_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S5D: Repartition",
          "id": "93",
          "inputStages": [
            "76",
            "17"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5666",
          "computeMsMax": "560985",
          "writeMsAvg": "727",
          "writeMsMax": "1038",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "99",
          "completedParallelInputs": "99",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage4C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S5E: Repartition",
          "id": "94",
          "inputStages": [
            "78",
            "19"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5752",
          "computeMsMax": "437178",
          "writeMsAvg": "736",
          "writeMsMax": "952",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "76",
          "completedParallelInputs": "76",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage4E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S5F: Repartition",
          "id": "95",
          "inputStages": [
            "75",
            "18"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5586",
          "computeMsMax": "340789",
          "writeMsAvg": "713",
          "writeMsMax": "1108",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "61",
          "completedParallelInputs": "61",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage4B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S60: Repartition",
          "id": "96",
          "inputStages": [
            "66",
            "15"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5827",
          "computeMsMax": "431216",
          "writeMsAvg": "738",
          "writeMsMax": "1066",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "74",
          "completedParallelInputs": "74",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage42_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S61: Repartition",
          "id": "97",
          "inputStages": [
            "45",
            "10"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5679",
          "computeMsMax": "505512",
          "writeMsAvg": "709",
          "writeMsMax": "1030",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "89",
          "completedParallelInputs": "89",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage2D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S62: Repartition",
          "id": "98",
          "inputStages": [
            "89",
            "31"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5735",
          "computeMsMax": "86031",
          "writeMsAvg": "726",
          "writeMsMax": "921",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "15",
          "completedParallelInputs": "15",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage59_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S63: Repartition",
          "id": "99",
          "inputStages": [
            "92",
            "13"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5342",
          "computeMsMax": "26713",
          "writeMsAvg": "718",
          "writeMsMax": "979",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "5",
          "completedParallelInputs": "5",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage5C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S64: Repartition",
          "id": "100",
          "inputStages": [
            "91",
            "16"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5772",
          "computeMsMax": "40405",
          "writeMsAvg": "708",
          "writeMsMax": "777",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage5B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S65: Repartition",
          "id": "101",
          "inputStages": [
            "90",
            "14"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5538",
          "computeMsMax": "49843",
          "writeMsAvg": "697",
          "writeMsMax": "766",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "9",
          "completedParallelInputs": "9",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage5A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S66: Repartition",
          "id": "102",
          "inputStages": [
            "94",
            "19"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5895",
          "computeMsMax": "47160",
          "writeMsAvg": "697",
          "writeMsMax": "788",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "8",
          "completedParallelInputs": "8",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage5E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S67: Repartition",
          "id": "103",
          "inputStages": [
            "96",
            "15"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5167",
          "computeMsMax": "31005",
          "writeMsAvg": "708",
          "writeMsMax": "991",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "6",
          "completedParallelInputs": "6",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage60_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S68: Repartition",
          "id": "104",
          "inputStages": [
            "93",
            "17"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5639",
          "computeMsMax": "39473",
          "writeMsAvg": "719",
          "writeMsMax": "882",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "7",
          "completedParallelInputs": "7",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage5D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S69: Repartition",
          "id": "105",
          "inputStages": [
            "95",
            "18"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5934",
          "computeMsMax": "53413",
          "writeMsAvg": "740",
          "writeMsMax": "838",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "9",
          "completedParallelInputs": "9",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage5F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S6A: Repartition",
          "id": "106",
          "inputStages": [
            "88",
            "12"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5651",
          "computeMsMax": "226051",
          "writeMsAvg": "726",
          "writeMsMax": "1018",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "40",
          "completedParallelInputs": "40",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage58_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S6B: Repartition",
          "id": "107",
          "inputStages": [
            "97",
            "10"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5546",
          "computeMsMax": "22186",
          "writeMsAvg": "735",
          "writeMsMax": "825",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "4",
          "completedParallelInputs": "4",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage61_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S6C: Repartition",
          "id": "108",
          "inputStages": [
            "87",
            "11"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5665",
          "computeMsMax": "175627",
          "writeMsAvg": "783",
          "writeMsMax": "954",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "31",
          "completedParallelInputs": "31",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage57_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S6D: Aggregate",
          "id": "109",
          "inputStages": [
            "30"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1598",
          "computeMsMax": "1598",
          "writeMsAvg": "66",
          "writeMsMax": "66",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$3060, $3061",
                "FROM __stage1E_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3080 := $3071"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3070 := $3060, $3071 := $3061"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3080",
                "TO __stage6D_output",
                "BY HASH($3080)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S6E: Aggregate",
          "id": "110",
          "inputStages": [
            "6"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2039",
          "computeMsMax": "2039",
          "writeMsAvg": "135",
          "writeMsMax": "135",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1850, $1851",
                "FROM __stage06_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1870 := $1861"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1860 := $1850, $1861 := $1851"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1870",
                "TO __stage6E_output",
                "BY HASH($1870)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S6F: Aggregate",
          "id": "111",
          "inputStages": [
            "4"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1967",
          "computeMsMax": "1967",
          "writeMsAvg": "90",
          "writeMsMax": "90",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2190, $2191",
                "FROM __stage04_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2210 := $2201"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2200 := $2190, $2201 := $2191"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2210",
                "TO __stage6F_output",
                "BY HASH($2210)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S70: Aggregate",
          "id": "112",
          "inputStages": [
            "3"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2072",
          "computeMsMax": "2072",
          "writeMsAvg": "102",
          "writeMsMax": "102",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2360, $2361",
                "FROM __stage03_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2380 := $2371"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2370 := $2360, $2371 := $2361"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2380",
                "TO __stage70_output",
                "BY HASH($2380)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S71: Coalesce",
          "id": "113",
          "inputStages": [
            "109"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "10",
          "computeMsMax": "1020",
          "writeMsAvg": "56",
          "writeMsMax": "83",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage6D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S72: Join+",
          "id": "114",
          "inputStages": [
            "98",
            "31",
            "113"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1331",
          "computeMsMax": "1308835",
          "writeMsAvg": "23",
          "writeMsMax": "204",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "983",
          "completedParallelInputs": "983",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$3040",
                "FROM __stage1F_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$3080",
                "FROM __stage71_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3100 := $3090"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $3050 = $3080"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3050 := $3040"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3100",
                "TO __stage72_output",
                "BY HASH($3100)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S73: Aggregate",
          "id": "115",
          "inputStages": [
            "9"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1720",
          "computeMsMax": "1720",
          "writeMsAvg": "83",
          "writeMsMax": "83",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1340, $1341",
                "FROM __stage09_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1360 := $1351"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1350 := $1340, $1351 := $1341"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1360",
                "TO __stage73_output",
                "BY HASH($1360)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S74: Aggregate",
          "id": "116",
          "inputStages": [
            "7"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2174",
          "computeMsMax": "2174",
          "writeMsAvg": "70",
          "writeMsMax": "70",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1680, $1681",
                "FROM __stage07_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1700 := $1691"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1690 := $1680, $1691 := $1681"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1700",
                "TO __stage74_output",
                "BY HASH($1700)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S75: Coalesce",
          "id": "117",
          "inputStages": [
            "111"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "917",
          "writeMsAvg": "78",
          "writeMsMax": "119",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage6F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S76: Join+",
          "id": "118",
          "inputStages": [
            "101",
            "14",
            "117"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1400",
          "computeMsMax": "1301196",
          "writeMsAvg": "16",
          "writeMsMax": "74",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "929",
          "completedParallelInputs": "929",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2170",
                "FROM __stage0E_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2210",
                "FROM __stage75_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2230 := $2220"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $2180 = $2210"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2180 := $2170"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2230",
                "TO __stage76_output",
                "BY HASH($2230)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S77: Aggregate",
          "id": "119",
          "inputStages": [
            "8"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2713",
          "computeMsMax": "2713",
          "writeMsAvg": "75",
          "writeMsMax": "75",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1510, $1511",
                "FROM __stage08_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1530 := $1521"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1520 := $1510, $1521 := $1511"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1530",
                "TO __stage77_output",
                "BY HASH($1530)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S78: Aggregate",
          "id": "120",
          "inputStages": [
            "5"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2261",
          "computeMsMax": "2261",
          "writeMsAvg": "72",
          "writeMsMax": "72",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2020, $2021",
                "FROM __stage05_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2040 := $2031"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2030 := $2020, $2031 := $2021"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2040",
                "TO __stage78_output",
                "BY HASH($2040)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S79: Coalesce",
          "id": "121",
          "inputStages": [
            "110"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8",
          "computeMsMax": "857",
          "writeMsAvg": "19",
          "writeMsMax": "63",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage6E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S7A: Join+",
          "id": "122",
          "inputStages": [
            "100",
            "16",
            "121"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1228",
          "computeMsMax": "1262722",
          "writeMsAvg": "15",
          "writeMsMax": "213",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1028",
          "completedParallelInputs": "1028",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1830",
                "FROM __stage10_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1870",
                "FROM __stage79_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1890 := $1880"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $1840 = $1870"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1840 := $1830"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1890",
                "TO __stage7A_output",
                "BY HASH($1890)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S7B: Aggregate",
          "id": "123",
          "inputStages": [
            "0"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2321",
          "computeMsMax": "2321",
          "writeMsAvg": "81",
          "writeMsMax": "81",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2870, $2871",
                "FROM __stage00_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2890 := $2881"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2880 := $2870, $2881 := $2871"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2890",
                "TO __stage7B_output",
                "BY HASH($2890)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S7C: Aggregate",
          "id": "124",
          "inputStages": [
            "2"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2292",
          "computeMsMax": "2292",
          "writeMsAvg": "86",
          "writeMsMax": "86",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2530, $2531",
                "FROM __stage02_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2550 := $2541"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2540 := $2530, $2541 := $2531"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2550",
                "TO __stage7C_output",
                "BY HASH($2550)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S7D: Repartition",
          "id": "125",
          "inputStages": [
            "114"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5721",
          "computeMsMax": "5721",
          "writeMsAvg": "504",
          "writeMsMax": "504",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage72_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S7E: Coalesce",
          "id": "126",
          "inputStages": [
            "112"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "951",
          "writeMsAvg": "61",
          "writeMsMax": "115",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage70_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S7F: Join+",
          "id": "127",
          "inputStages": [
            "99",
            "13",
            "126"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1382",
          "computeMsMax": "1284595",
          "writeMsAvg": "25",
          "writeMsMax": "127",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "929",
          "completedParallelInputs": "929",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2340",
                "FROM __stage0D_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2380",
                "FROM __stage7E_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2400 := $2390"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $2350 = $2380"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2350 := $2340"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2400",
                "TO __stage7F_output",
                "BY HASH($2400)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S80: Repartition",
          "id": "128",
          "inputStages": [
            "125",
            "114"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6031",
          "computeMsMax": "6031",
          "writeMsAvg": "573",
          "writeMsMax": "573",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage7D_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S81: Repartition",
          "id": "129",
          "inputStages": [
            "118"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4908",
          "computeMsMax": "4908",
          "writeMsAvg": "473",
          "writeMsMax": "473",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage76_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S82: Coalesce",
          "id": "130",
          "inputStages": [
            "115"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "910",
          "writeMsAvg": "75",
          "writeMsMax": "174",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage73_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S83: Join+",
          "id": "131",
          "inputStages": [
            "102",
            "19",
            "130"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1355",
          "computeMsMax": "1295536",
          "writeMsAvg": "43",
          "writeMsMax": "129",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "956",
          "completedParallelInputs": "956",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1320",
                "FROM __stage13_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1360",
                "FROM __stage82_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1380 := $1370"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $1330 = $1360"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1330 := $1320"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1380",
                "TO __stage83_output",
                "BY HASH($1380)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S84: Aggregate",
          "id": "132",
          "inputStages": [
            "1"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1775",
          "computeMsMax": "1775",
          "writeMsAvg": "74",
          "writeMsMax": "74",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2700, $2701",
                "FROM __stage01_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2720 := $2711"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2710 := $2700, $2711 := $2701"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2720",
                "TO __stage84_output",
                "BY HASH($2720)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S85: Coalesce",
          "id": "133",
          "inputStages": [
            "116"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "928",
          "writeMsAvg": "18",
          "writeMsMax": "106",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage74_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S86: Join+",
          "id": "134",
          "inputStages": [
            "104",
            "17",
            "133"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1153",
          "computeMsMax": "1331657",
          "writeMsAvg": "33",
          "writeMsMax": "113",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1154",
          "completedParallelInputs": "1154",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1660",
                "FROM __stage11_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1700",
                "FROM __stage85_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1720 := $1710"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $1670 = $1700"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1670 := $1660"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1720",
                "TO __stage86_output",
                "BY HASH($1720)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S87: Repartition",
          "id": "135",
          "inputStages": [
            "122"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6663",
          "computeMsMax": "13326",
          "writeMsAvg": "642",
          "writeMsMax": "653",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage7A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S88: Coalesce",
          "id": "136",
          "inputStages": [
            "120"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "956",
          "writeMsAvg": "93",
          "writeMsMax": "129",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage78_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S89: Join+",
          "id": "137",
          "inputStages": [
            "103",
            "15",
            "136"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1389",
          "computeMsMax": "1278400",
          "writeMsAvg": "52",
          "writeMsMax": "214",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "920",
          "completedParallelInputs": "920",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2000",
                "FROM __stage0F_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2040",
                "FROM __stage88_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2060 := $2050"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $2010 = $2040"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2010 := $2000"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2060",
                "TO __stage89_output",
                "BY HASH($2060)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S8A: Coalesce",
          "id": "138",
          "inputStages": [
            "119"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "926",
          "writeMsAvg": "34",
          "writeMsMax": "103",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage77_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S8B: Join+",
          "id": "139",
          "inputStages": [
            "105",
            "18",
            "138"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1558",
          "computeMsMax": "1293709",
          "writeMsAvg": "58",
          "writeMsMax": "387",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "830",
          "completedParallelInputs": "830",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1490",
                "FROM __stage12_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1530",
                "FROM __stage8A_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1550 := $1540"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $1500 = $1530"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1500 := $1490"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1550",
                "TO __stage8B_output",
                "BY HASH($1550)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S8C: Repartition",
          "id": "140",
          "inputStages": [
            "127"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6087",
          "computeMsMax": "12174",
          "writeMsAvg": "607",
          "writeMsMax": "687",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage7F_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S8D: Coalesce",
          "id": "141",
          "inputStages": [
            "123"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "948",
          "writeMsAvg": "40",
          "writeMsMax": "81",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage7B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S8E: Join+",
          "id": "142",
          "inputStages": [
            "107",
            "10",
            "141"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1271",
          "computeMsMax": "1318280",
          "writeMsAvg": "39",
          "writeMsMax": "146",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1037",
          "completedParallelInputs": "1037",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2850",
                "FROM __stage0A_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2890",
                "FROM __stage8D_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2910 := $2900"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $2860 = $2890"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2860 := $2850"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2910",
                "TO __stage8E_output",
                "BY HASH($2910)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S8F: Coalesce",
          "id": "143",
          "inputStages": [
            "124"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9",
          "computeMsMax": "930",
          "writeMsAvg": "38",
          "writeMsMax": "103",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage7C_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S90: Join+",
          "id": "144",
          "inputStages": [
            "106",
            "12",
            "143"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1230",
          "computeMsMax": "1308836",
          "writeMsAvg": "23",
          "writeMsMax": "126",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1064",
          "completedParallelInputs": "1064",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2510",
                "FROM __stage0C_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2550",
                "FROM __stage8F_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2570 := $2560"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $2520 = $2550"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2520 := $2510"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2570",
                "TO __stage90_output",
                "BY HASH($2570)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S91: Repartition",
          "id": "145",
          "inputStages": [
            "131"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6254",
          "computeMsMax": "6254",
          "writeMsAvg": "641",
          "writeMsMax": "641",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage83_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S92: Repartition",
          "id": "146",
          "inputStages": [
            "145",
            "131"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4492",
          "computeMsMax": "4492",
          "writeMsAvg": "436",
          "writeMsMax": "436",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage91_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S93: Repartition",
          "id": "147",
          "inputStages": [
            "134"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5754",
          "computeMsMax": "5754",
          "writeMsAvg": "505",
          "writeMsMax": "505",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage86_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S94: Repartition",
          "id": "148",
          "inputStages": [
            "147",
            "134"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7972",
          "computeMsMax": "7972",
          "writeMsAvg": "757",
          "writeMsMax": "757",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage93_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S95: Coalesce",
          "id": "149",
          "inputStages": [
            "132"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8",
          "computeMsMax": "862",
          "writeMsAvg": "33",
          "writeMsMax": "102",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage84_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S96: Join+",
          "id": "150",
          "inputStages": [
            "108",
            "11",
            "149"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1252",
          "computeMsMax": "1242485",
          "writeMsAvg": "24",
          "writeMsMax": "194",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "992",
          "completedParallelInputs": "992",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2680",
                "FROM __stage0B_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2720",
                "FROM __stage95_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2740 := $2730"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "ANTI HASH JOIN EACH  WITH ALL  ON $2690 = $2720"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2690 := $2680"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2740",
                "TO __stage96_output",
                "BY HASH($2740)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S97: Repartition",
          "id": "151",
          "inputStages": [
            "129",
            "118"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5979",
          "computeMsMax": "5979",
          "writeMsAvg": "556",
          "writeMsMax": "556",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage81_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S98: Repartition",
          "id": "152",
          "inputStages": [
            "137"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5815",
          "computeMsMax": "5815",
          "writeMsAvg": "486",
          "writeMsMax": "486",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage89_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S99: Repartition",
          "id": "153",
          "inputStages": [
            "139"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4444",
          "computeMsMax": "4444",
          "writeMsAvg": "542",
          "writeMsMax": "542",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage8B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S9A: Repartition",
          "id": "154",
          "inputStages": [
            "152",
            "137"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3391",
          "computeMsMax": "3391",
          "writeMsAvg": "399",
          "writeMsMax": "399",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage98_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S9B: Repartition",
          "id": "155",
          "inputStages": [
            "142"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6167",
          "computeMsMax": "6167",
          "writeMsAvg": "626",
          "writeMsMax": "626",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage8E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S9C: Repartition",
          "id": "156",
          "inputStages": [
            "155",
            "142"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6172",
          "computeMsMax": "6172",
          "writeMsAvg": "541",
          "writeMsMax": "541",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage9B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S9D: Repartition",
          "id": "157",
          "inputStages": [
            "144"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5958",
          "computeMsMax": "11916",
          "writeMsAvg": "559",
          "writeMsMax": "565",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "2",
          "completedParallelInputs": "2",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage90_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S9E: Repartition",
          "id": "158",
          "inputStages": [
            "150"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "5059",
          "computeMsMax": "5059",
          "writeMsAvg": "509",
          "writeMsMax": "509",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage96_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S9F: Repartition",
          "id": "159",
          "inputStages": [
            "158",
            "150"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8101",
          "computeMsMax": "8101",
          "writeMsAvg": "820",
          "writeMsMax": "820",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage9E_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA0: Repartition",
          "id": "160",
          "inputStages": [
            "153",
            "139"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4489",
          "computeMsMax": "4489",
          "writeMsAvg": "573",
          "writeMsMax": "573",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage99_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA1: Input",
          "id": "161",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "145",
          "readMsMax": "385",
          "computeMsAvg": "11258",
          "computeMsMax": "2262864",
          "writeMsAvg": "52",
          "writeMsMax": "407",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1:family_id, $2:fst_dt, $3:er_inpatient_hospitalization, $4:pregnancy",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1, $2, $3, $4",
                "TO __stageA1_output",
                "BY HASH($1)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA2: Aggregate",
          "id": "162",
          "inputStages": [
            "71",
            "42"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2292",
          "computeMsMax": "87103",
          "writeMsAvg": "31",
          "writeMsMax": "60",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "38",
          "completedParallelInputs": "38",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$3120",
                "FROM __stage2A_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3130 := $3120"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3130",
                "TO __stageA2_output",
                "BY HASH($3130)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA4: Input",
          "id": "164",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "127",
          "readMsMax": "315",
          "computeMsAvg": "10405",
          "computeMsMax": "2091492",
          "writeMsAvg": "300",
          "writeMsMax": "925",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$80:family_id, $81:Eligeff, $82:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$80, $81, $82",
                "TO __stageA4_output",
                "BY HASH($80)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA5: Aggregate",
          "id": "165",
          "inputStages": [
            "23"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4949",
          "computeMsMax": "98994",
          "writeMsAvg": "39",
          "writeMsMax": "52",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2420",
                "FROM __stage17_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2430 := $2420"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2430",
                "TO __stageA5_output",
                "BY HASH($2430)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA7: Input",
          "id": "167",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "69",
          "readMsMax": "339",
          "computeMsAvg": "10460",
          "computeMsMax": "2102530",
          "writeMsAvg": "296",
          "writeMsMax": "752",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$20:family_id, $21:Eligeff, $22:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$20, $21, $22",
                "TO __stageA7_output",
                "BY HASH($20)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA8: Aggregate",
          "id": "168",
          "inputStages": [
            "83",
            "29"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1804",
          "computeMsMax": "84825",
          "writeMsAvg": "33",
          "writeMsMax": "70",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "47",
          "completedParallelInputs": "47",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1400",
                "FROM __stage1D_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1410 := $1400"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1410",
                "TO __stageA8_output",
                "BY HASH($1410)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SA9: Aggregate",
          "id": "169",
          "inputStages": [
            "146",
            "131"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3744",
          "computeMsMax": "74894",
          "writeMsAvg": "214",
          "writeMsMax": "277",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1380",
                "FROM __stage83_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1390 := $1380"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1390",
                "TO __stageA9_output",
                "BY HASH($1390)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SAA: Input",
          "id": "170",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "53",
          "readMsMax": "249",
          "computeMsAvg": "10488",
          "computeMsMax": "2108131",
          "writeMsAvg": "283",
          "writeMsMax": "661",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$50:family_id, $51:Eligeff, $52:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$50, $51, $52",
                "TO __stageAA_output",
                "BY HASH($50)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SAB: Aggregate",
          "id": "171",
          "inputStages": [
            "84",
            "26"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1942",
          "computeMsMax": "91294",
          "writeMsAvg": "32",
          "writeMsMax": "179",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "47",
          "completedParallelInputs": "47",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1910",
                "FROM __stage1A_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1920 := $1910"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1920",
                "TO __stageAB_output",
                "BY HASH($1920)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SAC: Aggregate",
          "id": "172",
          "inputStages": [
            "135",
            "122"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3765",
          "computeMsMax": "75317",
          "writeMsAvg": "211",
          "writeMsMax": "285",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1890",
                "FROM __stage7A_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1900 := $1890"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1900",
                "TO __stageAC_output",
                "BY HASH($1900)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SAD: Input",
          "id": "173",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "51",
          "readMsMax": "307",
          "computeMsAvg": "10298",
          "computeMsMax": "2070083",
          "writeMsAvg": "292",
          "writeMsMax": "673",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$70:family_id, $71:Eligeff, $72:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$70, $71, $72",
                "TO __stageAD_output",
                "BY HASH($70)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SAE: Aggregate",
          "id": "174",
          "inputStages": [
            "24"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "4932",
          "computeMsMax": "98643",
          "writeMsAvg": "47",
          "writeMsMax": "100",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2250",
                "FROM __stage18_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2260 := $2250"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2260",
                "TO __stageAE_output",
                "BY HASH($2260)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SAF: Aggregate",
          "id": "175",
          "inputStages": [
            "151",
            "118"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3662",
          "computeMsMax": "73240",
          "writeMsAvg": "259",
          "writeMsMax": "536",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2230",
                "FROM __stage76_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2240 := $2230"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2240",
                "TO __stageAF_output",
                "BY HASH($2240)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB0: Input",
          "id": "176",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "54",
          "readMsMax": "205",
          "computeMsAvg": "10380",
          "computeMsMax": "2086453",
          "writeMsAvg": "282",
          "writeMsMax": "947",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$60:family_id, $61:Eligeff, $62:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$60, $61, $62",
                "TO __stageB0_output",
                "BY HASH($60)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB1: Aggregate",
          "id": "177",
          "inputStages": [
            "80",
            "25"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3371",
          "computeMsMax": "97777",
          "writeMsAvg": "36",
          "writeMsMax": "53",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "29",
          "completedParallelInputs": "29",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2080",
                "FROM __stage19_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2090 := $2080"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2090",
                "TO __stageB1_output",
                "BY HASH($2090)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB2: Aggregate",
          "id": "178",
          "inputStages": [
            "154",
            "137"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3893",
          "computeMsMax": "77879",
          "writeMsAvg": "238",
          "writeMsMax": "377",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2060",
                "FROM __stage89_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2070 := $2060"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2070",
                "TO __stageB2_output",
                "BY HASH($2070)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB3: Input",
          "id": "179",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "51",
          "readMsMax": "162",
          "computeMsAvg": "10435",
          "computeMsMax": "2097592",
          "writeMsAvg": "284",
          "writeMsMax": "680",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$90:family_id, $91:Eligeff, $92:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$90, $91, $92",
                "TO __stageB3_output",
                "BY HASH($90)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB4: Aggregate",
          "id": "180",
          "inputStages": [
            "82",
            "22"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2301",
          "computeMsMax": "87475",
          "writeMsAvg": "26",
          "writeMsMax": "72",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "38",
          "completedParallelInputs": "38",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2590",
                "FROM __stage16_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2600 := $2590"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2600",
                "TO __stageB4_output",
                "BY HASH($2600)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB5: Aggregate",
          "id": "181",
          "inputStages": [
            "157",
            "144"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3607",
          "computeMsMax": "72159",
          "writeMsAvg": "204",
          "writeMsMax": "254",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2570",
                "FROM __stage90_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2580 := $2570"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2580",
                "TO __stageB5_output",
                "BY HASH($2580)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB6: Input",
          "id": "182",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "49",
          "readMsMax": "276",
          "computeMsAvg": "10204",
          "computeMsMax": "2051086",
          "writeMsAvg": "280",
          "writeMsMax": "809",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$110:family_id, $111:Eligeff, $112:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$110, $111, $112",
                "TO __stageB6_output",
                "BY HASH($110)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB7: Aggregate",
          "id": "183",
          "inputStages": [
            "79",
            "20"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1992",
          "computeMsMax": "93653",
          "writeMsAvg": "20",
          "writeMsMax": "60",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "47",
          "completedParallelInputs": "47",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2930",
                "FROM __stage14_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2940 := $2930"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2940",
                "TO __stageB7_output",
                "BY HASH($2940)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB8: Aggregate",
          "id": "184",
          "inputStages": [
            "156",
            "142"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3670",
          "computeMsMax": "73409",
          "writeMsAvg": "214",
          "writeMsMax": "289",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2910",
                "FROM __stage8E_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2920 := $2910"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2920",
                "TO __stageB8_output",
                "BY HASH($2920)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SB9: Input",
          "id": "185",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "48",
          "readMsMax": "134",
          "computeMsAvg": "10276",
          "computeMsMax": "2065617",
          "writeMsAvg": "289",
          "writeMsMax": "622",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$30:family_id, $31:Eligeff, $32:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$30, $31, $32",
                "TO __stageB9_output",
                "BY HASH($30)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SBA: Aggregate",
          "id": "186",
          "inputStages": [
            "81",
            "28"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "2361",
          "computeMsMax": "89754",
          "writeMsAvg": "17",
          "writeMsMax": "51",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "38",
          "completedParallelInputs": "38",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1570",
                "FROM __stage1C_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1580 := $1570"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1580",
                "TO __stageBA_output",
                "BY HASH($1580)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SBB: Aggregate",
          "id": "187",
          "inputStages": [
            "160",
            "139"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3654",
          "computeMsMax": "73093",
          "writeMsAvg": "214",
          "writeMsMax": "322",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1550",
                "FROM __stage8B_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1560 := $1550"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1560",
                "TO __stageBB_output",
                "BY HASH($1560)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SBC: Input",
          "id": "188",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "50",
          "readMsMax": "263",
          "computeMsAvg": "10339",
          "computeMsMax": "2078244",
          "writeMsAvg": "296",
          "writeMsMax": "932",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$40:family_id, $41:Eligeff, $42:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$40, $41, $42",
                "TO __stageBC_output",
                "BY HASH($40)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SBD: Aggregate",
          "id": "189",
          "inputStages": [
            "85",
            "27"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1291",
          "computeMsMax": "83933",
          "writeMsAvg": "22",
          "writeMsMax": "56",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "65",
          "completedParallelInputs": "65",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1740",
                "FROM __stage1B_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1750 := $1740"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1750",
                "TO __stageBD_output",
                "BY HASH($1750)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SBE: Aggregate",
          "id": "190",
          "inputStages": [
            "148",
            "134"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3664",
          "computeMsMax": "73294",
          "writeMsAvg": "208",
          "writeMsMax": "273",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1720",
                "FROM __stage86_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1730 := $1720"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1730",
                "TO __stageBE_output",
                "BY HASH($1730)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SBF: Input",
          "id": "191",
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "76",
          "readMsMax": "387",
          "computeMsAvg": "10445",
          "computeMsMax": "2099594",
          "writeMsAvg": "292",
          "writeMsMax": "941",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "201",
          "completedParallelInputs": "201",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$100:family_id, $101:Eligeff, $102:Eligend",
                "FROM project_2907.67067"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$100, $101, $102",
                "TO __stageBF_output",
                "BY HASH($100)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC0: Aggregate",
          "id": "192",
          "inputStages": [
            "86",
            "21"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3368",
          "computeMsMax": "97697",
          "writeMsAvg": "19",
          "writeMsMax": "40",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "29",
          "completedParallelInputs": "29",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2760",
                "FROM __stage15_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2770 := $2760"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2770",
                "TO __stageC0_output",
                "BY HASH($2770)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC1: Aggregate",
          "id": "193",
          "inputStages": [
            "159",
            "150"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "3542",
          "computeMsMax": "70854",
          "writeMsAvg": "223",
          "writeMsMax": "348",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2740",
                "FROM __stage96_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2750 := $2740"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2750",
                "TO __stageC1_output",
                "BY HASH($2750)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC2: Coalesce",
          "id": "194",
          "inputStages": [
            "165"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "26",
          "computeMsMax": "2649",
          "writeMsAvg": "10",
          "writeMsMax": "61",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageA5_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC3: Join+",
          "id": "195",
          "inputStages": [
            "140",
            "127",
            "194"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6926",
          "computeMsMax": "138525",
          "writeMsAvg": "37",
          "writeMsMax": "51",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2400",
                "FROM __stage7F_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2430",
                "FROM __stageC2_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH ALL  ON $2410 = $2430"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2410 := $2400"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2440",
                "TO __stageC3_output",
                "BY HASH($2440)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC4: Join",
          "id": "196",
          "inputStages": [
            "174",
            "175"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "161",
          "computeMsMax": "16168",
          "writeMsAvg": "15",
          "writeMsMax": "27",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2240",
                "FROM __stageAF_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2260",
                "FROM __stageAE_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $2240 = $2260"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2270",
                "TO __stageC4_output",
                "BY HASH($2270)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC5: Join",
          "id": "197",
          "inputStages": [
            "168",
            "169"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "157",
          "computeMsMax": "15770",
          "writeMsAvg": "9",
          "writeMsMax": "70",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1390",
                "FROM __stageA9_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1410",
                "FROM __stageA8_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $1390 = $1410"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1420",
                "TO __stageC5_output",
                "BY HASH($1420)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC6: Repartition",
          "id": "198",
          "inputStages": [
            "196"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "36",
          "computeMsMax": "3681",
          "writeMsAvg": "8",
          "writeMsMax": "21",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageC4_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC7: Repartition",
          "id": "199",
          "inputStages": [
            "173"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8331",
          "computeMsMax": "833116",
          "writeMsAvg": "251",
          "writeMsMax": "439",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageAD_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC8: Repartition",
          "id": "200",
          "inputStages": [
            "197"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "40",
          "computeMsMax": "4039",
          "writeMsAvg": "9",
          "writeMsMax": "20",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageC5_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SC9: Repartition",
          "id": "201",
          "inputStages": [
            "167"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8431",
          "computeMsMax": "843145",
          "writeMsAvg": "257",
          "writeMsMax": "456",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageA7_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SCA: Join+",
          "id": "202",
          "inputStages": [
            "199",
            "198",
            "196",
            "173"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "591",
          "computeMsMax": "591868",
          "writeMsAvg": "71",
          "writeMsMax": "561",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$70, $71, $72",
                "FROM __stageAD_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2270",
                "FROM __stageC4_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2290 := $2280, $2291 := $2281, $2292 := $2282"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $70 = $2270"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2290, $2291, $2292",
                "TO __stageCA_output",
                "BY HASH($2290, $2291, $2292)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SCB: Join",
          "id": "203",
          "inputStages": [
            "177",
            "178"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "165",
          "computeMsMax": "16599",
          "writeMsAvg": "8",
          "writeMsMax": "90",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2070",
                "FROM __stageB2_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2090",
                "FROM __stageB1_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $2070 = $2090"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2100",
                "TO __stageCB_output",
                "BY HASH($2100)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SCC: Coalesce",
          "id": "204",
          "inputStages": [
            "162"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "27",
          "computeMsMax": "2730",
          "writeMsAvg": "13",
          "writeMsMax": "77",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageA2_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SCD: Join+",
          "id": "205",
          "inputStages": [
            "128",
            "114",
            "204"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "7679",
          "computeMsMax": "153596",
          "writeMsAvg": "39",
          "writeMsMax": "59",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "20",
          "completedParallelInputs": "20",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$3100",
                "FROM __stage72_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$3130",
                "FROM __stageCC_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH ALL  ON $3110 = $3130"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3110 := $3100"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3140",
                "TO __stageCD_output",
                "BY HASH($3140)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SCE: Join+",
          "id": "206",
          "inputStages": [
            "201",
            "200",
            "197",
            "167"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "570",
          "computeMsMax": "570232",
          "writeMsAvg": "29",
          "writeMsMax": "169",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$20, $21, $22",
                "FROM __stageA7_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1420",
                "FROM __stageC5_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1440 := $1430, $1441 := $1431, $1442 := $1432"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $20 = $1420"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1440, $1441, $1442",
                "TO __stageCE_output",
                "BY HASH($1440, $1441, $1442)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SCF: Join",
          "id": "207",
          "inputStages": [
            "180",
            "181"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "160",
          "computeMsMax": "16091",
          "writeMsAvg": "7",
          "writeMsMax": "24",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2580",
                "FROM __stageB5_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2600",
                "FROM __stageB4_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $2580 = $2600"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2610",
                "TO __stageCF_output",
                "BY HASH($2610)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD0: Join",
          "id": "208",
          "inputStages": [
            "186",
            "187"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "163",
          "computeMsMax": "16377",
          "writeMsAvg": "8",
          "writeMsMax": "21",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1560",
                "FROM __stageBB_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1580",
                "FROM __stageBA_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $1560 = $1580"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1590",
                "TO __stageD0_output",
                "BY HASH($1590)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD1: Join",
          "id": "209",
          "inputStages": [
            "189",
            "190"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "161",
          "computeMsMax": "16113",
          "writeMsAvg": "7",
          "writeMsMax": "22",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1730",
                "FROM __stageBE_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1750",
                "FROM __stageBD_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $1730 = $1750"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1760",
                "TO __stageD1_output",
                "BY HASH($1760)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD2: Join",
          "id": "210",
          "inputStages": [
            "171",
            "172"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "158",
          "computeMsMax": "15853",
          "writeMsAvg": "7",
          "writeMsMax": "19",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1900",
                "FROM __stageAC_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1920",
                "FROM __stageAB_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $1900 = $1920"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1930",
                "TO __stageD2_output",
                "BY HASH($1930)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD3: Join",
          "id": "211",
          "inputStages": [
            "183",
            "184"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "163",
          "computeMsMax": "16314",
          "writeMsAvg": "9",
          "writeMsMax": "136",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2920",
                "FROM __stageB8_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2940",
                "FROM __stageB7_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $2920 = $2940"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2950",
                "TO __stageD3_output",
                "BY HASH($2950)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD4: Repartition",
          "id": "212",
          "inputStages": [
            "195"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "35",
          "computeMsMax": "3599",
          "writeMsAvg": "8",
          "writeMsMax": "16",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageC3_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD5: Repartition",
          "id": "213",
          "inputStages": [
            "164"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8359",
          "computeMsMax": "835919",
          "writeMsAvg": "259",
          "writeMsMax": "522",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageA4_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD6: Join",
          "id": "214",
          "inputStages": [
            "192",
            "193"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "166",
          "computeMsMax": "16608",
          "writeMsAvg": "9",
          "writeMsMax": "72",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2750",
                "FROM __stageC1_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2770",
                "FROM __stageC0_output"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $2750 = $2770"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2780",
                "TO __stageD6_output",
                "BY HASH($2780)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD7: Repartition",
          "id": "215",
          "inputStages": [
            "203"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "40",
          "computeMsMax": "4063",
          "writeMsAvg": "9",
          "writeMsMax": "15",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageCB_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD8: Repartition",
          "id": "216",
          "inputStages": [
            "176"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8858",
          "computeMsMax": "885886",
          "writeMsAvg": "250",
          "writeMsMax": "450",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageB0_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SD9: Repartition",
          "id": "217",
          "inputStages": [
            "207"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "40",
          "computeMsMax": "4031",
          "writeMsAvg": "14",
          "writeMsMax": "23",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageCF_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SDA: Repartition",
          "id": "218",
          "inputStages": [
            "179"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8709",
          "computeMsMax": "870903",
          "writeMsAvg": "273",
          "writeMsMax": "512",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageB3_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SDB: Repartition",
          "id": "219",
          "inputStages": [
            "209"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "39",
          "computeMsMax": "3958",
          "writeMsAvg": "16",
          "writeMsMax": "40",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageD1_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SDC: Repartition",
          "id": "220",
          "inputStages": [
            "188"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8575",
          "computeMsMax": "857580",
          "writeMsAvg": "258",
          "writeMsMax": "496",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageBC_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SDD: Repartition",
          "id": "221",
          "inputStages": [
            "208"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "40",
          "computeMsMax": "4020",
          "writeMsAvg": "9",
          "writeMsMax": "19",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageD0_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SDE: Repartition",
          "id": "222",
          "inputStages": [
            "185"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8448",
          "computeMsMax": "844843",
          "writeMsAvg": "253",
          "writeMsMax": "553",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageB9_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SDF: Join+",
          "id": "223",
          "inputStages": [
            "213",
            "212",
            "195",
            "164"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "590",
          "computeMsMax": "590282",
          "writeMsAvg": "43",
          "writeMsMax": "291",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$80, $81, $82",
                "FROM __stageA4_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2440",
                "FROM __stageC3_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2460 := $2450, $2461 := $2451, $2462 := $2452"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $80 = $2440"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2460, $2461, $2462",
                "TO __stageDF_output",
                "BY HASH($2460, $2461, $2462)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE0: Repartition",
          "id": "224",
          "inputStages": [
            "210"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "41",
          "computeMsMax": "4115",
          "writeMsAvg": "9",
          "writeMsMax": "31",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageD2_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE1: Repartition",
          "id": "225",
          "inputStages": [
            "170"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8542",
          "computeMsMax": "854236",
          "writeMsAvg": "258",
          "writeMsMax": "490",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageAA_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE2: Repartition",
          "id": "226",
          "inputStages": [
            "211"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "41",
          "computeMsMax": "4157",
          "writeMsAvg": "10",
          "writeMsMax": "25",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageD3_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE3: Repartition",
          "id": "227",
          "inputStages": [
            "182"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8651",
          "computeMsMax": "865164",
          "writeMsAvg": "271",
          "writeMsMax": "520",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageB6_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE4: Join+",
          "id": "228",
          "inputStages": [
            "220",
            "219",
            "209",
            "188"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "596",
          "computeMsMax": "596821",
          "writeMsAvg": "55",
          "writeMsMax": "565",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$40, $41, $42",
                "FROM __stageBC_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1760",
                "FROM __stageD1_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1780 := $1770, $1781 := $1771, $1782 := $1772"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $40 = $1760"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1780, $1781, $1782",
                "TO __stageE4_output",
                "BY HASH($1780, $1781, $1782)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE5: Join+",
          "id": "229",
          "inputStages": [
            "218",
            "217",
            "207",
            "179"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "597",
          "computeMsMax": "597060",
          "writeMsAvg": "80",
          "writeMsMax": "382",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$90, $91, $92",
                "FROM __stageB3_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2610",
                "FROM __stageCF_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2630 := $2620, $2631 := $2621, $2632 := $2622"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $90 = $2610"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2630, $2631, $2632",
                "TO __stageE5_output",
                "BY HASH($2630, $2631, $2632)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE6: Join+",
          "id": "230",
          "inputStages": [
            "216",
            "215",
            "203",
            "176"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "591",
          "computeMsMax": "591402",
          "writeMsAvg": "9",
          "writeMsMax": "70",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$60, $61, $62",
                "FROM __stageB0_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2100",
                "FROM __stageCB_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2120 := $2110, $2121 := $2111, $2122 := $2112"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $60 = $2100"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2120, $2121, $2122",
                "TO __stageE6_output",
                "BY HASH($2120, $2121, $2122)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE7: Repartition",
          "id": "231",
          "inputStages": [
            "214"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "40",
          "computeMsMax": "4070",
          "writeMsAvg": "12",
          "writeMsMax": "84",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageD6_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE8: Repartition",
          "id": "232",
          "inputStages": [
            "191"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8851",
          "computeMsMax": "885189",
          "writeMsAvg": "274",
          "writeMsMax": "717",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageBF_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SE9: Join+",
          "id": "233",
          "inputStages": [
            "222",
            "221",
            "208",
            "185"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "592",
          "computeMsMax": "592809",
          "writeMsAvg": "129",
          "writeMsMax": "695",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$30, $31, $32",
                "FROM __stageB9_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1590",
                "FROM __stageD0_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1610 := $1600, $1611 := $1601, $1612 := $1602"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $30 = $1590"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1610, $1611, $1612",
                "TO __stageE9_output",
                "BY HASH($1610, $1611, $1612)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SEA: Join+",
          "id": "234",
          "inputStages": [
            "225",
            "224",
            "210",
            "170"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "592",
          "computeMsMax": "592640",
          "writeMsAvg": "58",
          "writeMsMax": "166",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$50, $51, $52",
                "FROM __stageAA_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1930",
                "FROM __stageD2_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1950 := $1940, $1951 := $1941, $1952 := $1942"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $50 = $1930"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1950, $1951, $1952",
                "TO __stageEA_output",
                "BY HASH($1950, $1951, $1952)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SEB: Join+",
          "id": "235",
          "inputStages": [
            "227",
            "226",
            "211",
            "182"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "596",
          "computeMsMax": "596865",
          "writeMsAvg": "98",
          "writeMsMax": "403",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$110, $111, $112",
                "FROM __stageB6_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2950",
                "FROM __stageD3_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2970 := $2960, $2971 := $2961, $2972 := $2962"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $110 = $2950"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2970, $2971, $2972",
                "TO __stageEB_output",
                "BY HASH($2970, $2971, $2972)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SEC: Join+",
          "id": "236",
          "inputStages": [
            "232",
            "231",
            "214",
            "191"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "595",
          "computeMsMax": "595456",
          "writeMsAvg": "48",
          "writeMsMax": "156",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$100, $101, $102",
                "FROM __stageBF_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2780",
                "FROM __stageD6_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2800 := $2790, $2801 := $2791, $2802 := $2792"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $100 = $2780"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2800, $2801, $2802",
                "TO __stageEC_output",
                "BY HASH($2800, $2801, $2802)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SED: Repartition",
          "id": "237",
          "inputStages": [
            "205"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "37",
          "computeMsMax": "3771",
          "writeMsAvg": "9",
          "writeMsMax": "35",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageCD_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SEE: Repartition",
          "id": "238",
          "inputStages": [
            "161"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "6292",
          "computeMsMax": "629291",
          "writeMsAvg": "60",
          "writeMsMax": "198",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "100",
          "completedParallelInputs": "100",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageA1_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SEF: Join+",
          "id": "239",
          "inputStages": [
            "238",
            "237",
            "205",
            "161"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "857",
          "computeMsMax": "857217",
          "writeMsAvg": "24",
          "writeMsMax": "152",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1000",
          "completedParallelInputs": "1000",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1, $2, $3, $4",
                "FROM __stageA1_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$3140",
                "FROM __stageCD_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3160 := $3150, $3161 := $3151, $3162 := $3153, $3163 := $3152"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "INNER HASH JOIN EACH  WITH EACH  ON $1 = $3140"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3160, $3161, $3163, $3162",
                "TO __stageEF_output",
                "BY HASH($3160, $3161, $3162, $3163)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF0: Aggregate",
          "id": "240",
          "inputStages": [
            "32"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1581",
          "computeMsMax": "1581",
          "writeMsAvg": "58",
          "writeMsMax": "58",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2990, $2991",
                "FROM __stage20_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $3000 := $2990, $3001 := $2991"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$3000",
                "TO __stageF0_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF1: Aggregate",
          "id": "241",
          "inputStages": [
            "235"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "705",
          "computeMsMax": "15522",
          "writeMsAvg": "63",
          "writeMsMax": "126",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2970, $2971, $2972",
                "FROM __stageEB_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2980 := $2970, $2981 := $2971, $2982 := $2972"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2980, $2981, $2982",
                "TO __stageF1_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF2: Aggregate",
          "id": "242",
          "inputStages": [
            "33"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1681",
          "computeMsMax": "1681",
          "writeMsAvg": "76",
          "writeMsMax": "76",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2820, $2821",
                "FROM __stage21_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2830 := $2820, $2831 := $2821"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2830",
                "TO __stageF2_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF3: Aggregate",
          "id": "243",
          "inputStages": [
            "236"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "730",
          "computeMsMax": "16068",
          "writeMsAvg": "57",
          "writeMsMax": "90",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2800, $2801, $2802",
                "FROM __stageEC_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2810 := $2800, $2811 := $2801, $2812 := $2802"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2810, $2811, $2812",
                "TO __stageF3_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF4: Aggregate",
          "id": "244",
          "inputStages": [
            "34"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1611",
          "computeMsMax": "1611",
          "writeMsAvg": "61",
          "writeMsMax": "61",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2650, $2651",
                "FROM __stage22_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2660 := $2650, $2661 := $2651"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2660",
                "TO __stageF4_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF5: Aggregate",
          "id": "245",
          "inputStages": [
            "229"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "671",
          "computeMsMax": "14769",
          "writeMsAvg": "38",
          "writeMsMax": "48",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2630, $2631, $2632",
                "FROM __stageE5_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2640 := $2630, $2641 := $2631, $2642 := $2632"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2640, $2641, $2642",
                "TO __stageF5_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF6: Aggregate",
          "id": "246",
          "inputStages": [
            "35"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1499",
          "computeMsMax": "1499",
          "writeMsAvg": "56",
          "writeMsMax": "56",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2480, $2481",
                "FROM __stage23_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2490 := $2480, $2491 := $2481"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2490",
                "TO __stageF6_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF7: Aggregate",
          "id": "247",
          "inputStages": [
            "223"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "690",
          "computeMsMax": "15190",
          "writeMsAvg": "42",
          "writeMsMax": "59",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2460, $2461, $2462",
                "FROM __stageDF_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2470 := $2460, $2471 := $2461, $2472 := $2462"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2470, $2471, $2472",
                "TO __stageF7_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF8: Aggregate",
          "id": "248",
          "inputStages": [
            "36"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1951",
          "computeMsMax": "1951",
          "writeMsAvg": "66",
          "writeMsMax": "66",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2310, $2311",
                "FROM __stage24_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2320 := $2310, $2321 := $2311"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2320",
                "TO __stageF8_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SF9: Aggregate",
          "id": "249",
          "inputStages": [
            "202"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "669",
          "computeMsMax": "14722",
          "writeMsAvg": "46",
          "writeMsMax": "94",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2290, $2291, $2292",
                "FROM __stageCA_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2300 := $2290, $2301 := $2291, $2302 := $2292"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2300, $2301, $2302",
                "TO __stageF9_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SFA: Aggregate",
          "id": "250",
          "inputStages": [
            "37"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1593",
          "computeMsMax": "1593",
          "writeMsAvg": "65",
          "writeMsMax": "65",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2140, $2141",
                "FROM __stage25_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2150 := $2140, $2151 := $2141"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2150",
                "TO __stageFA_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SFB: Aggregate",
          "id": "251",
          "inputStages": [
            "230"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "663",
          "computeMsMax": "14600",
          "writeMsAvg": "46",
          "writeMsMax": "78",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$2120, $2121, $2122",
                "FROM __stageE6_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $2130 := $2120, $2131 := $2121, $2132 := $2122"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$2130, $2131, $2132",
                "TO __stageFB_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SFC: Aggregate",
          "id": "252",
          "inputStages": [
            "38"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1611",
          "computeMsMax": "1611",
          "writeMsAvg": "59",
          "writeMsMax": "59",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1970, $1971",
                "FROM __stage26_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1980 := $1970, $1981 := $1971"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1980",
                "TO __stageFC_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SFD: Aggregate",
          "id": "253",
          "inputStages": [
            "234"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "683",
          "computeMsMax": "15027",
          "writeMsAvg": "38",
          "writeMsMax": "53",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1950, $1951, $1952",
                "FROM __stageEA_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1960 := $1950, $1961 := $1951, $1962 := $1952"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1960, $1961, $1962",
                "TO __stageFD_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SFE: Aggregate",
          "id": "254",
          "inputStages": [
            "39"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1646",
          "computeMsMax": "1646",
          "writeMsAvg": "67",
          "writeMsMax": "67",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1800, $1801",
                "FROM __stage27_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1810 := $1800, $1811 := $1801"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1810",
                "TO __stageFE_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "SFF: Aggregate",
          "id": "255",
          "inputStages": [
            "228"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "702",
          "computeMsMax": "15463",
          "writeMsAvg": "37",
          "writeMsMax": "55",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1780, $1781, $1782",
                "FROM __stageE4_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1790 := $1780, $1791 := $1781, $1792 := $1782"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1790, $1791, $1792",
                "TO __stageFF_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S100: Aggregate",
          "id": "256",
          "inputStages": [
            "40"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1769",
          "computeMsMax": "1769",
          "writeMsAvg": "81",
          "writeMsMax": "81",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1630, $1631",
                "FROM __stage28_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1640 := $1630, $1641 := $1631"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1640",
                "TO __stage100_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S101: Aggregate",
          "id": "257",
          "inputStages": [
            "233"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "706",
          "computeMsMax": "15547",
          "writeMsAvg": "48",
          "writeMsMax": "156",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1610, $1611, $1612",
                "FROM __stageE9_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1620 := $1610, $1621 := $1611, $1622 := $1612"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1620, $1621, $1622",
                "TO __stage101_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S102: Aggregate",
          "id": "258",
          "inputStages": [
            "41"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "1608",
          "computeMsMax": "1608",
          "writeMsAvg": "75",
          "writeMsMax": "75",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "1",
          "completedParallelInputs": "1",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1460, $1461",
                "FROM __stage29_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1470 := $1460, $1471 := $1461"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1470",
                "TO __stage102_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S103: Aggregate",
          "id": "259",
          "inputStages": [
            "206"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "673",
          "computeMsMax": "14826",
          "writeMsAvg": "40",
          "writeMsMax": "54",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "22",
          "completedParallelInputs": "22",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1440, $1441, $1442",
                "FROM __stageCE_output"
              ]
            },
            {
              "kind": "AGGREGATE",
              "substeps": [
                "GROUP BY $1450 := $1440, $1451 := $1441, $1452 := $1442"
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$1450, $1451, $1452",
                "TO __stage103_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S104: Repartition",
          "id": "260",
          "inputStages": [
            "239"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9414",
          "computeMsMax": "103557",
          "writeMsAvg": "83",
          "writeMsMax": "392",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "11",
          "completedParallelInputs": "11",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stageEF_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S105: Join+",
          "id": "261",
          "inputStages": [
            "240",
            "243",
            "248",
            "257",
            "253",
            "249",
            "251",
            "254",
            "241",
            "244",
            "247",
            "255",
            "242",
            "250",
            "246",
            "256",
            "245",
            "252",
            "258",
            "259"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "0",
          "computeMsMax": "0",
          "writeMsAvg": "0",
          "writeMsMax": "0",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "16000",
          "completedParallelInputs": "0",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "$1450, $1451, $1452",
                "FROM __stage103_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1470",
                "FROM __stage102_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1620, $1621, $1622",
                "FROM __stage101_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1640",
                "FROM __stage100_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1790, $1791, $1792",
                "FROM __stageFF_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1810",
                "FROM __stageFE_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1960, $1961, $1962",
                "FROM __stageFD_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$1980",
                "FROM __stageFC_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2130, $2131, $2132",
                "FROM __stageFB_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2150",
                "FROM __stageFA_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2300, $2301, $2302",
                "FROM __stageF9_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2320",
                "FROM __stageF8_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2470, $2471, $2472",
                "FROM __stageF7_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2490",
                "FROM __stageF6_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2640, $2641, $2642",
                "FROM __stageF5_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2660",
                "FROM __stageF4_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2810, $2811, $2812",
                "FROM __stageF3_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2830",
                "FROM __stageF2_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$2980, $2981, $2982",
                "FROM __stageF1_output"
              ]
            },
            {
              "kind": "READ",
              "substeps": [
                "$3000",
                "FROM __stageF0_output"
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$590 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($1483, $1481, $1482)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$600 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($1653, $1651, $1652)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$610 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($1823, $1821, $1822)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$620 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($1993, $1991, $1992)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$630 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($2163, $2161, $2162)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$640 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($2333, $2331, $2332)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$650 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($2503, $2501, $2502)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$660 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($2673, $2671, $2672)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$670 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($2843, $2841, $2842)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "COMPUTE",
              "substeps": [
                "$680 := rand()"
              ]
            },
            {
              "kind": "FILTER",
              "substeps": [
                "between($3013, $3011, $3012)"
              ]
            },
            {
              "kind": "JOIN",
              "substeps": [
                "CROSS EACH  WITH EACH "
              ]
            },
            {
              "kind": "WRITE",
              "substeps": [
                "$580, $581, $582",
                "TO __stage105_output",
                "BY HASH($580)"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S106: Output",
          "id": "262",
          "inputStages": [
            "260",
            "239"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "8701",
          "computeMsMax": "95717",
          "writeMsAvg": "76",
          "writeMsMax": "157",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "11",
          "completedParallelInputs": "11",
          "status": "COMPLETE",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage104_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S107: Repartition",
          "id": "263",
          "inputStages": [
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "12681",
          "computeMsMax": "710136",
          "writeMsAvg": "5073",
          "writeMsMax": "7952",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "56",
          "completedParallelInputs": "52",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage105_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S108: Repartition",
          "id": "264",
          "inputStages": [
            "263",
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "12665",
          "computeMsMax": "734576",
          "writeMsAvg": "5095",
          "writeMsMax": "13660",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "58",
          "completedParallelInputs": "49",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage107_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S109: Repartition",
          "id": "265",
          "inputStages": [
            "264",
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "9158",
          "computeMsMax": "512864",
          "writeMsAvg": "3421",
          "writeMsMax": "13019",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "56",
          "completedParallelInputs": "31",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage108_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S10A: Repartition",
          "id": "266",
          "inputStages": [
            "265",
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "0",
          "computeMsMax": "0",
          "writeMsAvg": "0",
          "writeMsMax": "0",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "50",
          "completedParallelInputs": "0",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage109_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S10B: Repartition",
          "id": "267",
          "inputStages": [
            "266",
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "0",
          "computeMsMax": "0",
          "writeMsAvg": "0",
          "writeMsMax": "0",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "551",
          "completedParallelInputs": "0",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage10A_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S10C: Repartition",
          "id": "268",
          "inputStages": [
            "267",
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "0",
          "computeMsMax": "0",
          "writeMsAvg": "0",
          "writeMsMax": "0",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "551",
          "completedParallelInputs": "0",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage10B_output"
              ]
            }
          ],
          "slotMs": "0"
        },
        {
          "name": "S10D: Output",
          "id": "269",
          "inputStages": [
            "268",
            "261"
          ],
          "waitMsAvg": "0",
          "waitMsMax": "0",
          "readMsAvg": "0",
          "readMsMax": "0",
          "computeMsAvg": "0",
          "computeMsMax": "0",
          "writeMsAvg": "0",
          "writeMsMax": "0",
          "shuffleOutputBytes": "0",
          "shuffleOutputBytesSpilled": "0",
          "recordsRead": "0",
          "recordsWritten": "0",
          "parallelInputs": "551",
          "completedParallelInputs": "0",
          "status": "RUNNING",
          "steps": [
            {
              "kind": "READ",
              "substeps": [
                "FROM __stage10C_output"
              ]
            }
          ],
          "slotMs": "0"
        }
      ],
      "estimatedBytesProcessed": "96369723291",
      "timeline": [
        {
          "elapsedMs": "736",
          "totalSlotMs": "4078",
          "pendingUnits": "3437",
          "completedUnits": "0",
          "activeUnits": "45"
        },
        {
          "elapsedMs": "1770",
          "totalSlotMs": "242395",
          "pendingUnits": "4442",
          "completedUnits": "22",
          "activeUnits": "941"
        },
        {
          "elapsedMs": "2783",
          "totalSlotMs": "3764566",
          "pendingUnits": "4422",
          "completedUnits": "42",
          "activeUnits": "3462"
        },
        {
          "elapsedMs": "3820",
          "totalSlotMs": "13839894",
          "pendingUnits": "4304",
          "completedUnits": "160",
          "activeUnits": "4459"
        },
        {
          "elapsedMs": "4841",
          "totalSlotMs": "24372972",
          "pendingUnits": "3685",
          "completedUnits": "852",
          "activeUnits": "4046"
        },
        {
          "elapsedMs": "5890",
          "totalSlotMs": "32489460",
          "pendingUnits": "2450",
          "completedUnits": "2240",
          "activeUnits": "2944"
        },
        {
          "elapsedMs": "6934",
          "totalSlotMs": "38758274",
          "pendingUnits": "2451",
          "completedUnits": "2255",
          "activeUnits": "2493"
        },
        {
          "elapsedMs": "8054",
          "totalSlotMs": "45916122",
          "pendingUnits": "2398",
          "completedUnits": "2308",
          "activeUnits": "2480"
        },
        {
          "elapsedMs": "9087",
          "totalSlotMs": "52436082",
          "pendingUnits": "2052",
          "completedUnits": "2654",
          "activeUnits": "2337"
        },
        {
          "elapsedMs": "10105",
          "totalSlotMs": "57276681",
          "pendingUnits": "1321",
          "completedUnits": "3385",
          "activeUnits": "1761"
        },
        {
          "elapsedMs": "11131",
          "totalSlotMs": "60193602",
          "pendingUnits": "742",
          "completedUnits": "4077",
          "activeUnits": "995"
        },
        {
          "elapsedMs": "12146",
          "totalSlotMs": "61704571",
          "pendingUnits": "687",
          "completedUnits": "4536",
          "activeUnits": "568"
        },
        {
          "elapsedMs": "13170",
          "totalSlotMs": "63085410",
          "pendingUnits": "864",
          "completedUnits": "4659",
          "activeUnits": "798"
        },
        {
          "elapsedMs": "14196",
          "totalSlotMs": "65204236",
          "pendingUnits": "836",
          "completedUnits": "4687",
          "activeUnits": "871"
        },
        {
          "elapsedMs": "15224",
          "totalSlotMs": "67285630",
          "pendingUnits": "722",
          "completedUnits": "4801",
          "activeUnits": "792"
        },
        {
          "elapsedMs": "16261",
          "totalSlotMs": "68867734",
          "pendingUnits": "381",
          "completedUnits": "5157",
          "activeUnits": "558"
        },
        {
          "elapsedMs": "17300",
          "totalSlotMs": "69599396",
          "pendingUnits": "110",
          "completedUnits": "5457",
          "activeUnits": "298"
        },
        {
          "elapsedMs": "18373",
          "totalSlotMs": "69903381",
          "pendingUnits": "112",
          "completedUnits": "5521",
          "activeUnits": "165"
        },
        {
          "elapsedMs": "19506",
          "totalSlotMs": "70237415",
          "pendingUnits": "132",
          "completedUnits": "5532",
          "activeUnits": "140"
        },
        {
          "elapsedMs": "20583",
          "totalSlotMs": "70562044",
          "pendingUnits": "96",
          "completedUnits": "5571",
          "activeUnits": "119"
        },
        {
          "elapsedMs": "21639",
          "totalSlotMs": "70958387",
          "pendingUnits": "1026",
          "completedUnits": "5727",
          "activeUnits": "1026"
        },
        {
          "elapsedMs": "22886",
          "totalSlotMs": "75997300",
          "pendingUnits": "2370",
          "completedUnits": "8633",
          "activeUnits": "2745"
        },
        {
          "elapsedMs": "23988",
          "totalSlotMs": "84740338",
          "pendingUnits": "4529",
          "completedUnits": "11987",
          "activeUnits": "5535"
        },
        {
          "elapsedMs": "25143",
          "totalSlotMs": "97268578",
          "pendingUnits": "1117",
          "completedUnits": "16501",
          "activeUnits": "3912"
        },
        {
          "elapsedMs": "26208",
          "totalSlotMs": "104181071",
          "pendingUnits": "811",
          "completedUnits": "16807",
          "activeUnits": "2817"
        },
        {
          "elapsedMs": "27366",
          "totalSlotMs": "109204416",
          "pendingUnits": "581",
          "completedUnits": "17297",
          "activeUnits": "2006"
        },
        {
          "elapsedMs": "28320",
          "totalSlotMs": "111636678",
          "pendingUnits": "1461",
          "completedUnits": "17685",
          "activeUnits": "1478"
        },
        {
          "elapsedMs": "29487",
          "totalSlotMs": "115770464",
          "pendingUnits": "2140",
          "completedUnits": "17792",
          "activeUnits": "2132"
        },
        {
          "elapsedMs": "30529",
          "totalSlotMs": "121519884",
          "pendingUnits": "2235",
          "completedUnits": "17983",
          "activeUnits": "2314"
        },
        {
          "elapsedMs": "32001",
          "totalSlotMs": "130442714",
          "pendingUnits": "3644",
          "completedUnits": "18624",
          "activeUnits": "2449"
        },
        {
          "elapsedMs": "33017",
          "totalSlotMs": "136874660",
          "pendingUnits": "4425",
          "completedUnits": "19143",
          "activeUnits": "2752"
        },
        {
          "elapsedMs": "34520",
          "totalSlotMs": "145481176",
          "pendingUnits": "3675",
          "completedUnits": "20193",
          "activeUnits": "2360"
        },
        {
          "elapsedMs": "35040",
          "totalSlotMs": "147515319",
          "pendingUnits": "3412",
          "completedUnits": "20456",
          "activeUnits": "1688"
        },
        {
          "elapsedMs": "36548",
          "totalSlotMs": "152365945",
          "pendingUnits": "3193",
          "completedUnits": "21175",
          "activeUnits": "1565"
        },
        {
          "elapsedMs": "37547",
          "totalSlotMs": "155931077",
          "pendingUnits": "3005",
          "completedUnits": "21363",
          "activeUnits": "2012"
        },
        {
          "elapsedMs": "38548",
          "totalSlotMs": "160396040",
          "pendingUnits": "7920",
          "completedUnits": "24648",
          "activeUnits": "4148"
        },
        {
          "elapsedMs": "38940",
          "totalSlotMs": "162712857",
          "pendingUnits": "7912",
          "completedUnits": "24656",
          "activeUnits": "4452"
        },
        {
          "elapsedMs": "40324",
          "totalSlotMs": "177684725",
          "pendingUnits": "8811",
          "completedUnits": "24757",
          "activeUnits": "8831"
        },
        {
          "elapsedMs": "41389",
          "totalSlotMs": "191629578",
          "pendingUnits": "8782",
          "completedUnits": "24786",
          "activeUnits": "8820"
        },
        {
          "elapsedMs": "42486",
          "totalSlotMs": "205343135",
          "pendingUnits": "8739",
          "completedUnits": "25029",
          "activeUnits": "8735"
        },
        {
          "elapsedMs": "43680",
          "totalSlotMs": "219340721",
          "pendingUnits": "8318",
          "completedUnits": "25450",
          "activeUnits": "8343"
        },
        {
          "elapsedMs": "45141",
          "totalSlotMs": "231994543",
          "pendingUnits": "1165",
          "completedUnits": "32603",
          "activeUnits": "7081"
        },
        {
          "elapsedMs": "46171",
          "totalSlotMs": "234656791",
          "pendingUnits": "2102",
          "completedUnits": "32666",
          "activeUnits": "2108"
        },
        {
          "elapsedMs": "47397",
          "totalSlotMs": "238788729",
          "pendingUnits": "2090",
          "completedUnits": "32678",
          "activeUnits": "2106"
        },
        {
          "elapsedMs": "48598",
          "totalSlotMs": "242029614",
          "pendingUnits": "1147",
          "completedUnits": "33851",
          "activeUnits": "1264"
        },
        {
          "elapsedMs": "50104",
          "totalSlotMs": "245277080",
          "pendingUnits": "16093",
          "completedUnits": "34927",
          "activeUnits": "5159"
        },
        {
          "elapsedMs": "51106",
          "totalSlotMs": "256138946",
          "pendingUnits": "16192",
          "completedUnits": "34998",
          "activeUnits": "13807"
        },
        {
          "elapsedMs": "51920",
          "totalSlotMs": "277321581",
          "pendingUnits": "16242",
          "completedUnits": "34998",
          "activeUnits": "16287"
        },
        {
          "elapsedMs": "52952",
          "totalSlotMs": "306190433",
          "pendingUnits": "16793",
          "completedUnits": "34998",
          "activeUnits": "16316"
        },
        {
          "elapsedMs": "54387",
          "totalSlotMs": "342632355",
          "pendingUnits": "17338",
          "completedUnits": "35004",
          "activeUnits": "16315"
        },
        {
          "elapsedMs": "55558",
          "totalSlotMs": "371188166",
          "pendingUnits": "17875",
          "completedUnits": "35018",
          "activeUnits": "16305"
        },
        {
          "elapsedMs": "56741",
          "totalSlotMs": "403935218",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16310"
        },
        {
          "elapsedMs": "57825",
          "totalSlotMs": "441924032",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16344"
        },
        {
          "elapsedMs": "58921",
          "totalSlotMs": "479802083",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16369"
        },
        {
          "elapsedMs": "60227",
          "totalSlotMs": "522697511",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16383"
        },
        {
          "elapsedMs": "61303",
          "totalSlotMs": "562348832",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16408"
        },
        {
          "elapsedMs": "62687",
          "totalSlotMs": "600372381",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16416"
        },
        {
          "elapsedMs": "64290",
          "totalSlotMs": "647869832",
          "pendingUnits": "17874",
          "completedUnits": "35019",
          "activeUnits": "16422"
        },
        {
          "elapsedMs": "65474",
          "totalSlotMs": "688908393",
          "pendingUnits": "17873",
          "completedUnits": "35020",
          "activeUnits": "16432"
        },
        {
          "elapsedMs": "66642",
          "totalSlotMs": "722194580",
          "pendingUnits": "17859",
          "completedUnits": "35034",
          "activeUnits": "16432"
        },
        {
          "elapsedMs": "67742",
          "totalSlotMs": "759397223",
          "pendingUnits": "17851",
          "completedUnits": "35042",
          "activeUnits": "16430"
        },
        {
          "elapsedMs": "68885",
          "totalSlotMs": "798152358",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16445"
        },
        {
          "elapsedMs": "69995",
          "totalSlotMs": "826774013",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16452"
        },
        {
          "elapsedMs": "71254",
          "totalSlotMs": "855331813",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16464"
        },
        {
          "elapsedMs": "72742",
          "totalSlotMs": "888885047",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16464"
        },
        {
          "elapsedMs": "73743",
          "totalSlotMs": "912273672",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16464"
        },
        {
          "elapsedMs": "75743",
          "totalSlotMs": "958033812",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16464"
        },
        {
          "elapsedMs": "76478",
          "totalSlotMs": "975198750",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16467"
        },
        {
          "elapsedMs": "78743",
          "totalSlotMs": "1029758156",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16467"
        },
        {
          "elapsedMs": "80742",
          "totalSlotMs": "1079348404",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16467"
        },
        {
          "elapsedMs": "82744",
          "totalSlotMs": "1128225890",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16467"
        },
        {
          "elapsedMs": "84744",
          "totalSlotMs": "1175815599",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16467"
        },
        {
          "elapsedMs": "86745",
          "totalSlotMs": "1222803202",
          "pendingUnits": "17849",
          "completedUnits": "35044",
          "activeUnits": "16467"
        },
        {
          "elapsedMs": "88272",
          "totalSlotMs": "1260280019",
          "pendingUnits": "17834",
          "completedUnits": "35059",
          "activeUnits": "16465"
        },
        {
          "elapsedMs": "89383",
          "totalSlotMs": "1297406600",
          "pendingUnits": "17819",
          "completedUnits": "35074",
          "activeUnits": "16444"
        },
        {
          "elapsedMs": "90706",
          "totalSlotMs": "1339542115",
          "pendingUnits": "17809",
          "completedUnits": "35084",
          "activeUnits": "16438"
        },
        {
          "elapsedMs": "91819",
          "totalSlotMs": "1373352030",
          "pendingUnits": "17806",
          "completedUnits": "35087",
          "activeUnits": "16486"
        },
        {
          "elapsedMs": "92969",
          "totalSlotMs": "1404901015",
          "pendingUnits": "17795",
          "completedUnits": "35098",
          "activeUnits": "16793"
        },
        {
          "elapsedMs": "94055",
          "totalSlotMs": "1439099149",
          "pendingUnits": "17789",
          "completedUnits": "35104",
          "activeUnits": "16891"
        },
        {
          "elapsedMs": "95471",
          "totalSlotMs": "1482287868",
          "pendingUnits": "17774",
          "completedUnits": "35119",
          "activeUnits": "16886"
        },
        {
          "elapsedMs": "96745",
          "totalSlotMs": "1520503214",
          "pendingUnits": "17773",
          "completedUnits": "35120",
          "activeUnits": "16880"
        },
        {
          "elapsedMs": "98215",
          "totalSlotMs": "1557480054",
          "pendingUnits": "17756",
          "completedUnits": "35137",
          "activeUnits": "16869"
        },
        {
          "elapsedMs": "99495",
          "totalSlotMs": "1594876663",
          "pendingUnits": "17756",
          "completedUnits": "35137",
          "activeUnits": "16869"
        },
        {
          "elapsedMs": "101745",
          "totalSlotMs": "1647277263",
          "pendingUnits": "17756",
          "completedUnits": "35137",
          "activeUnits": "16869"
        },
        {
          "elapsedMs": "103293",
          "totalSlotMs": "1682307375",
          "pendingUnits": "17747",
          "completedUnits": "35146",
          "activeUnits": "16861"
        },
        {
          "elapsedMs": "104748",
          "totalSlotMs": "1723929695",
          "pendingUnits": "17746",
          "completedUnits": "35147",
          "activeUnits": "16868"
        },
        {
          "elapsedMs": "105749",
          "totalSlotMs": "1745294178",
          "pendingUnits": "17746",
          "completedUnits": "35147",
          "activeUnits": "16868"
        },
        {
          "elapsedMs": "107351",
          "totalSlotMs": "1780309907",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "16866"
        },
        {
          "elapsedMs": "109746",
          "totalSlotMs": "1841890826",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15975"
        },
        {
          "elapsedMs": "110746",
          "totalSlotMs": "1865197297",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15975"
        },
        {
          "elapsedMs": "111746",
          "totalSlotMs": "1894707851",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15975"
        },
        {
          "elapsedMs": "112747",
          "totalSlotMs": "1917790424",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15546"
        },
        {
          "elapsedMs": "114748",
          "totalSlotMs": "1971207746",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15546"
        },
        {
          "elapsedMs": "116748",
          "totalSlotMs": "2015871002",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15546"
        },
        {
          "elapsedMs": "118748",
          "totalSlotMs": "2066751562",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15332"
        },
        {
          "elapsedMs": "119749",
          "totalSlotMs": "2086375729",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15332"
        },
        {
          "elapsedMs": "120751",
          "totalSlotMs": "2106486011",
          "pendingUnits": "17741",
          "completedUnits": "35152",
          "activeUnits": "15332"
        }
      ],
      "totalSlotMs": "2106486011",
      "statementType": "SELECT"
    },
    "totalSlotMs": "2106486011"
  },
  "status": {
    "errorResult": {
      "reason": "quotaExceeded",
      "message": "Quota exceeded: Your project exceeded quota for total shuffle size limit. For more information, see https://cloud.google.com/bigquery/troubleshooting-errors"
    },
    "errors": [
      {
        "reason": "quotaExceeded",
        "message": "Quota exceeded: Your project exceeded quota for total shuffle size limit. For more information, see https://cloud.google.com/bigquery/troubleshooting-errors"
      }
    ],
    "state": "DONE"
  }
}
