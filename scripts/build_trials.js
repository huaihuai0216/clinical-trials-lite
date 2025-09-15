// scripts/build_trials.js  (streaming 版)
const fs = require('fs');
const fsp = require('fs/promises');
const path = require('path');
const readline = require('readline');
const { chain } = require('stream-chain');
const { parser } = require('stream-json');
const { streamArray } = require('stream-json/streamers/StreamArray');

const INPUT_DIR = process.argv[2] || 'input';
const OUTPUT_DIR = process.argv[3] || 'dist';
const TRIALS_DIR = path.join(OUTPUT_DIR, 'trials');

const ensureDir = (p) => fs.mkdirSync(p, { recursive: true });

const toISO = (s) => {
  if (!s) return null;
  const m = String(s).match(/^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?/);
  if (!m) return null;
  const y = m[1], mo = m[2] || null, d = m[3] || null;
  return d ? `${y}-${mo}-${d}` : (mo ? `${y}-${mo}` : y);
};
const uniq = (arr) => Array.from(new Set((arr || []).filter(Boolean)));
const take = (arr, n) => (Array.isArray(arr) ? arr.slice(0, n) : []);

const buildIndexEntry = (rec) => {
  const p = rec.protocolSection || {};
  const idm = p.identificationModule || {};
  const nctId = idm.nctId || idm.orgStudyId || null;
  if (!nctId) return { nctId: null };

  const statusM = p.statusModule || {};
  const condM = p.conditionsModule || {};
  const armsM  = p.armsInterventionsModule || {};
  const design = p.designModule || {};

  const enrollment = (design.enrollmentInfo && design.enrollmentInfo.count)
    ? Number(design.enrollmentInfo.count) : null;

  const interventions = uniq(
    (armsM.interventions || []).map(x => x.name).map(n => {
      if (!n) return null;
      if (/oxybat/i.test(n) || /xyrem/i.test(n)) return 'Sodium Oxybate';
      if (/modafinil/i.test(n) || /provigil/i.test(n)) return 'Modafinil';
      return n;
    })
  );

  const locations = (p.contactsLocationsModule?.locations || []);
  const countries = uniq(locations.map(s => s.country).filter(Boolean));
  const siteCount = locations.length;

  const entry = {
    id: nctId,
    title: (idm.briefTitle || '').replace(/\s+/g,' ').trim().slice(0,140),
    status: statusM.overallStatus || null,
    phase: (design.phases && design.phases[0]) || null,
    start: toISO(statusM.startDateStruct?.date),
    primaryCompletion: toISO(statusM.primaryCompletionDateStruct?.date),
    completion: toISO(statusM.completionDateStruct?.date),
    enrollment,
    conditions: take(condM.conditions || [], 6),
    interventions: take(interventions, 6),
    locations: { countries: take(countries, 10), siteCount },
    arms: (armsM.armGroups || []).length || null,
    hasResults: !!rec.hasResults
  };
  return { nctId, entry };
};

const buildDetail = (rec, nctId) => {
  const p = rec.protocolSection || {};
  const statusM = p.statusModule || {};
  const armsM  = p.armsInterventionsModule || {};
  const design = p.designModule || {};
  const descM = p.descriptionModule || {};
  const outcomeM = p.outcomesModule || {};

  const resultsOutcomeM = rec.resultsSection?.outcomeMeasuresModule?.outcomeMeasures?.find(o => o.type === 'PRIMARY');
  const enrollment = (design.enrollmentInfo && design.enrollmentInfo.count)
    ? Number(design.enrollmentInfo.count) : null;

  const locations = (p.contactsLocationsModule?.locations || []);
  const countries = uniq(locations.map(s => s.country).filter(Boolean));
  const siteCount = locations.length;

  const detail = {
    id: nctId,
    sponsor: p.sponsorCollaboratorsModule?.leadSponsor?.name || null,
    briefTitle: p.identificationModule?.briefTitle || null,
    officialTitle: p.identificationModule?.officialTitle || null,
    status: statusM.overallStatus || null,
    phase: (design.phases && design.phases[0]) || null,
    dates: {
      start: toISO(statusM.startDateStruct?.date),
      primaryCompletion: toISO(statusM.primaryCompletionDateStruct?.date),
      completion: toISO(statusM.completionDateStruct?.date),
      firstPosted: toISO(statusM.studyFirstPostDateStruct?.date),
      resultsPosted: toISO(statusM.resultsFirstPostDateStruct?.date),
      lastUpdate: toISO(statusM.lastUpdatePostDateStruct?.date)
    },
    enrollment,
    design: {
      allocation: design.designInfo?.allocation || null,
      model: design.designInfo?.interventionModel || null,
      masking: design.designInfo?.maskingInfo?.masking || null,
      primaryPurpose: design.designInfo?.primaryPurpose || null
    },
    conditions: p.conditionsModule?.conditions || [],
    interventions: (armsM.interventions || []).map(iv => ({
      type: iv.type, name: iv.name, otherNames: iv.otherNames || []
    })),
    arms: (armsM.armGroups || []).map(a => ({ label: a.label, type: a.type })),
    summary: (descM.briefSummary || '').trim() || null,
    primaryOutcome: (() => {
      if (resultsOutcomeM) {
        const cats = resultsOutcomeM.classes?.[0]?.categories?.[0]?.measurements || [];
        const mapArmTitle = (gid) =>
          (resultsOutcomeM.groups || []).find(g => g.id === gid)?.title || gid;
        const effectByArm = cats.map(m => ({
          arm: mapArmTitle(m.groupId).replace(/\s+/g,' ').trim(),
          deltaMin: Number(m.value),
          sd: Number(m.spread)
        }));
        return {
          measure: resultsOutcomeM.title,
          timeFrame: resultsOutcomeM.timeFrame,
          effectByArm,
          pValues: (resultsOutcomeM.analyses || []).map(a => ({
            comp: (a.groupIds || []).join(' vs '),
            p: a.pValue || null
          }))
        };
      }
      const po = outcomeM.primaryOutcomes?.[0];
      return po ? { measure: po.measure, timeFrame: po.timeFrame } : null;
    })(),
    eligibility: {
      minAge: p.eligibilityModule?.minimumAge || null,
      sex: p.eligibilityModule?.sex || null,
      healthyVolunteers: !!p.eligibilityModule?.healthyVolunteers,
      inclusionCount: (p.eligibilityModule?.eligibilityCriteria || '').split(/INCLUSION CRITERIA/i)[1]?.split(/EXCLUSION CRITERIA/i)[0]?.split(/\n\*/).length-1 || null,
      exclusionCount: (p.eligibilityModule?.eligibilityCriteria || '').split(/EXCLUSION CRITERIA/i)[1]?.split(/\n\*/).length-1 || null
    },
    locations: { siteCount, countries },
    refs: {
      pmids: (p.referencesModule?.references || []).map(r => r.pmid).filter(Boolean),
      seeAlso: (p.referencesModule?.seeAlsoLinks || []).map(l => l.url)
    },
    adverseEventsSummary: (() => {
      const eg = rec.resultsSection?.adverseEventsModule?.eventGroups || [];
      const seriousAny = eg[0] ? { events: eg[0].seriousNumAffected, n: eg[0].seriousNumAtRisk } : null;
      const commons = rec.resultsSection?.adverseEventsModule?.otherEvents || [];
      const top = (commons || [])
        .map(e => ({ term: e.term, total: (e.stats||[]).reduce((s,x)=>s+(x.numAffected||0),0) }))
        .sort((a,b)=>b.total-a.total)
        .slice(0,5)
        .map(x=>x.term);
      return { seriousAny, commonTop: top };
    })()
  };

  return detail;
};

async function writeDetail(rec) {
  const { nctId, entry } = buildIndexEntry(rec);
  if (!nctId) return null;
  const detail = buildDetail(rec, nctId);
  const bucket = nctId.slice(0,4).toUpperCase();
  const dir = path.join(TRIALS_DIR, bucket);
  ensureDir(dir);
  await fsp.writeFile(path.join(dir, `${nctId}.json`), JSON.stringify(detail));
  return entry;
}

async function processJsonArrayFile(filePath, pushIndex) {
  // 大型 JSON 陣列：串流一筆一筆
  const pipeline = chain([fs.createReadStream(filePath), parser(), streamArray()]);
  for await (const { value } of pipeline) {
    try {
      const entry = await writeDetail(value);
      if (entry) pushIndex(entry);
    } catch (e) {
      console.error('❌ 解析失敗（陣列元素）:', e.message);
    }
  }
}

async function processNdjsonFile(filePath, pushIndex) {
  // NDJSON/JSONL：逐行
  const rl = readline.createInterface({
    input: fs.createReadStream(filePath),
    crlfDelay: Infinity
  });
  for await (const line of rl) {
    const s = line.trim();
    if (!s) continue;
    try {
      const obj = JSON.parse(s);
      const entry = await writeDetail(obj);
      if (entry) pushIndex(entry);
    } catch (e) {
      console.error('❌ 解析失敗（NDJSON）:', e.message);
    }
  }
}

async function processSingleJsonObject(filePath, pushIndex) {
  // 單筆物件（小檔）
  const raw = await fsp.readFile(filePath, 'utf8');
  const obj = JSON.parse(raw);
  const entry = await writeDetail(obj);
  if (entry) pushIndex(entry);
}

async function main() {
  if (!fs.existsSync(INPUT_DIR)) {
    console.error('❌ 找不到 input 目錄：', INPUT_DIR);
    process.exit(1);
  }
  ensureDir(OUTPUT_DIR);
  ensureDir(TRIALS_DIR);

  const files = fs.readdirSync(INPUT_DIR).filter(f => f.endsWith('.json') || f.endsWith('.ndjson') || f.endsWith('.jsonl'));
  if (files.length === 0) {
    console.warn('⚠️ input 內沒有 .json/.ndjson/.jsonl 檔案');
    return;
  }

  const index = [];
  const ingredientMap = new Map();
  // 小工具：正規化名稱（全小寫、去掉非英數）
  function normName(s) {
    return String(s).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  }

  // ✅ 新增：把一筆 index entry 的 interventions 加進倒排表
  function addIngredients(entry) {
    if (!entry || !entry.id) return;
    for (const raw of (entry.interventions || [])) {
      const key = normName(raw);
      if (!key) continue;
      if (!ingredientMap.has(key)) ingredientMap.set(key, new Set());
      ingredientMap.get(key).add(entry.id);
    }
  }
  const pushIndex = (entry) => {
    index.push(entry);
    addIngredients(entry); // ✅ 新增：同步更新成分倒排表
  };
  

  console.log(`📦 準備處理 ${files.length} 個檔案…`);

  for (const f of files) {
    const filePath = path.join(INPUT_DIR, f);
    console.log(`➡️  處理：${f}`);
    // 讀首 512 bytes 判斷格式
    const fd = fs.openSync(filePath, 'r');
    const buf = Buffer.alloc(512);
    const bytes = fs.readSync(fd, buf, 0, 512, 0);
    fs.closeSync(fd);
    const head = buf.toString('utf8', 0, bytes).trim();
    const firstChar = head[0];

    try {
      if (f.endsWith('.ndjson') || f.endsWith('.jsonl')) {
        await processNdjsonFile(filePath, pushIndex);
      } else if (firstChar === '[') {
        await processJsonArrayFile(filePath, pushIndex);
      } else if (firstChar === '{') {
        await processSingleJsonObject(filePath, pushIndex);
      } else {
        // 嘗試當 NDJSON
        await processNdjsonFile(filePath, pushIndex);
      }
    } catch (e) {
      console.error('❌ 檔案處理失敗：', f, e.message);
    }
  }

  // 排序並寫索引
  index.sort((a,b) => String(b.completion||'').localeCompare(String(a.completion||'')));
  await fsp.writeFile(path.join(OUTPUT_DIR, 'trials.index.json'), JSON.stringify(index));
  // ✅ 新增：輸出成分倒排表 facets/ingredients.json
  const facetsDir = path.join(OUTPUT_DIR, 'facets');
  ensureDir(facetsDir);
  const ingredientsObj = {};
  for (const [k, set] of ingredientMap.entries()) {
    ingredientsObj[k] = Array.from(set); // Set -> Array
  }
  await fsp.writeFile(
    path.join(facetsDir, 'ingredients.json'),
    JSON.stringify(ingredientsObj)
  );

  console.log(`✅ 完成！索引筆數：${index.length}`);
  console.log(`📁 輸出：${OUTPUT_DIR}/trials.index.json 與 ${OUTPUT_DIR}/trials/<bucket>/<NCTID>.json`);
}

main().catch(e => {
  console.error('💥 例外：', e);
  process.exit(1);
});
