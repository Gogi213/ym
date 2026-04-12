create or replace view public.goal_mapping_wide as
with topics as (
  select distinct matched_topic as topic
  from public.ingest_files
  where status = 'ingested'
)
select
  topics.topic,
  max(case when tgs.goal_slot = 1 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_1,
  max(case when tgs.goal_slot = 2 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_2,
  max(case when tgs.goal_slot = 3 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_3,
  max(case when tgs.goal_slot = 4 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_4,
  max(case when tgs.goal_slot = 5 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_5,
  max(case when tgs.goal_slot = 6 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_6,
  max(case when tgs.goal_slot = 7 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_7,
  max(case when tgs.goal_slot = 8 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_8,
  max(case when tgs.goal_slot = 9 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_9,
  max(case when tgs.goal_slot = 10 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_10,
  max(case when tgs.goal_slot = 11 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_11,
  max(case when tgs.goal_slot = 12 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_12,
  max(case when tgs.goal_slot = 13 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_13,
  max(case when tgs.goal_slot = 14 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_14,
  max(case when tgs.goal_slot = 15 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_15,
  max(case when tgs.goal_slot = 16 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_16,
  max(case when tgs.goal_slot = 17 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_17,
  max(case when tgs.goal_slot = 18 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_18,
  max(case when tgs.goal_slot = 19 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_19,
  max(case when tgs.goal_slot = 20 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_20,
  max(case when tgs.goal_slot = 21 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_21,
  max(case when tgs.goal_slot = 22 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_22,
  max(case when tgs.goal_slot = 23 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_23,
  max(case when tgs.goal_slot = 24 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_24,
  max(case when tgs.goal_slot = 25 then coalesce(tgs.goal_label, tgs.source_header) end) as goal_25
from topics
left join public.topic_goal_slots tgs
  on tgs.topic = topics.topic
group by topics.topic;
