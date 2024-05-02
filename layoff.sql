use layoff;
select *
from layoffs;

create table layoffs1
like layoffs;

insert layoffs1
select *
from layoffs;

select *
from layoffs1;

select *, row_number ( ) over (partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs1;

with duplicate_cte as
(select *, row_number ( ) over (partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs1)
select *
from duplicate_cte
where row_num>1;

CREATE TABLE `layoffs2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert layoffs2
select *, row_number ( ) over (partition by company, location, industry, total_laid_off, percentage_laid_off, 'date', stage, country, funds_raised_millions) as row_num
from layoffs1;

select *
from layoffs2;

delete
from layoffs2
where row_num>1;

select company, trim(company)
from layoffs2;

update layoffs2
set company = trim(company);

select *
from layoffs2;

select country, trim(trailing '.' from company)
from layoffs2
where country ='united states%';

select country
from layoffs2
where country = 'united states';

update layoffs2
set country = 'united states'
where country = 'united states%';

select *
from layoffs2;

select `date`, str_to_date(`date`, '%m/%d/%Y')
from layoffs2;

update layoffs2
set `date` = str_to_date(`date`, '%m/%d/%Y');

update layoffs2
set industry = 'crypto'
where industry = 'crypto%';

alter table layoffs2
modify column `date` date;

select *
from layoffs2;


select company, industry
from layoffs2
where industry is null or industry = '';

select company, industry
from layoffs2
where company = 'airbnb';

update layoffs2
set industry = null
where industry = '';

select *
from layoffs2 l1
join layoffs2 l2
on l1.company = l2.company 
and l1.location = l2.location
where l1.industry is null
and l2.industry is not null; 

update layoffs2 l1
join layoffs2 l2
on l1.company = l2.company 
set l1.industry=l2.industry
where l1.industry is null
and l2.industry is not null; 

select *
from layoffs2
where total_laid_off is null 
and percentage_laid_off is null;

delete 
from layoffs2
where total_laid_off is null 
and percentage_laid_off is null;

alter table layoffs2
drop column row_num;

select *
from layoffs2;

select max(total_laid_off), max(percentage_laid_off)
from layoffs2;

select *
from layoffs2
where percentage_laid_off= 1
order by total_laid_off desc;

select company, sum(total_laid_off)
from layoffs2
group by company
order by 2 desc;

select min(`date`), max(`date`)
from layoffs2;

select industry, sum(total_laid_off)
from layoffs2
group by industry
order by 2 desc;

select country, sum(total_laid_off)
from layoffs2
group by country
order by 2 desc;

select year(`date`), sum(total_laid_off)
from layoffs2
group by  year(`date`)
order by 1 desc;

select stage, sum(total_laid_off)
from layoffs2
group by  stage
order by 2 desc;

select year(`date`), month(`date`), sum(total_laid_off)
 from layoffs2
group by  year(`date`),month(`date`)
order by 1 asc;

#Finding rolling sum

select substring(`date`,1,7) as `month`, sum(total_laid_off)
 from layoffs2
group by  `month`
order by 1 desc;

with rolling_total as
(select substring(`date`,1,7) as `month`, sum(total_laid_off) as total
 from layoffs2
 where substring(`date`,1,7) is not null
group by  `month`
order by 1 asc)
select `month`, total,sum(total) over(order by `month`) as rolling_total
from rolling_total;

# To find the ranks of company based on total laid off

select company, year(`date`), sum(total_laid_off)
from layoffs2
group by company, year(`date`)
order by 3 desc;

# using cte to give ranks to companies based on their total_laid_off by years and arranging ranks

with company_year(company, years, total_laid_off)as
(
select company, year(`date`), sum(total_laid_off)
from layoffs2
group by company, year(`date`)
), company_rank as
(
select *, dense_rank() over(partition by years order by total_laid_off desc )as ranking
from company_year
where years is not null
)
select *
from company_rank
where ranking <=5;