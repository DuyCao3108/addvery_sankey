with segment_pivot as
 (select t.id_cuid
        ,max(case
               when t.reporting_date = date '2024-06-01' then
                t.client_addvery_segment
               else
                null
             end) as client_addvery_segment_jun
        ,max(case
               when t.reporting_date = date '2024-07-01' then
                t.client_addvery_segment
               else
                null
             end) as client_addvery_segment_jul
        ,max(case
               when t.reporting_date = date '2024-08-01' then
                t.client_addvery_segment
               else
                null
             end) as client_addvery_segment_aug
        ,max(case
               when t.reporting_date = date '2024-09-01' then
                t.client_addvery_segment
               else
                null
             end) as client_addvery_segment_sep
        ,max(case
               when t.reporting_date = date '2024-10-01' then
                t.client_addvery_segment
               else
                null
             end) as client_addvery_segment_oct
        ,max(case
               when t.reporting_date = date '2024-11-01' then
                t.client_addvery_segment
               else
                null
             end) as client_addvery_segment_nov
  from   AP_CRM.GTT_ADDVERY_FINAL_BASE_CH_SEGMENT t
  group  by t.id_cuid)
select count(*)
      ,case
         when s.client_addvery_segment_jun is null
              and s.client_addvery_segment_jul is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_jun, 'Signed in the Future')
       end as client_addvery_segment_jun
      ,case
         when s.client_addvery_segment_jul is null
              and s.client_addvery_segment_aug is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_jul, 'Signed in the Future')
       end as client_addvery_segment_jul
      ,case
         when s.client_addvery_segment_aug is null
              and s.client_addvery_segment_sep is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_aug, 'Signed in the Future')
       end as client_addvery_segment_aug
      ,case
         when s.client_addvery_segment_sep is null
              and s.client_addvery_segment_oct is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_sep, 'Signed in the Future')
       end as client_addvery_segment_sep 
      ,case
         when s.client_addvery_segment_oct is null
              and s.client_addvery_segment_nov is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_oct, 'Signed in the Future')
       end as client_addvery_segment_oct 
      ,s.client_addvery_segment_nov
from   segment_pivot s
where client_addvery_segment_jun is not null
group  by case
         when s.client_addvery_segment_jun is null
              and s.client_addvery_segment_jul is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_jun, 'Signed in the Future')
       end --as client_addvery_segment_jun
      ,case
         when s.client_addvery_segment_jul is null
              and s.client_addvery_segment_aug is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_jul, 'Signed in the Future')
       end --as client_addvery_segment_jul
      ,case
         when s.client_addvery_segment_aug is null
              and s.client_addvery_segment_sep is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_aug, 'Signed in the Future')
       end --as client_addvery_segment_aug
      ,case
         when s.client_addvery_segment_sep is null
              and s.client_addvery_segment_oct is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_sep, 'Signed in the Future')
       end --as client_addvery_segment_sep 
      ,case
         when s.client_addvery_segment_oct is null
              and s.client_addvery_segment_nov is not null then
          'New Signed next month'
         else
          nvl(s.client_addvery_segment_oct, 'Signed in the Future')
       end --as client_addvery_segment_oct 
      ,s.client_addvery_segment_nov
