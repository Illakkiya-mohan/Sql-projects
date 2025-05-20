-- Data cleaning

SELECT *
FROM layoffs;

-- 1. Remove duplicates
-- 2. Standardize the data
-- 3. Null values or blank values
-- 4. Remove any columns

-- STAGING IS LIKE COPING THE DATA FOR BACKUP AND IT WILL NOT AFFECT THE ORIGINAL DATA
CREATE TABLE Layoffs_staging
like layoffs;

INSERT Layoffs_staging
SELECT *
FROM layoffs;

-- 1. REMOVING DUPLICATES

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) as row_num
FROM Layoffs_staging;

with duplicate_cte as
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM Layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffs_staging2` (
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

SELECT *
FROM layoffs_staging2;

insert into layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, 
percentage_laid_off, `date`, stage, country, funds_raised_millions) as row_num
FROM Layoffs_staging;

DELETE
FROM layoffs_staging2
where row_num > 1;

SELECT *
FROM layoffs_staging2;

-- standardizing data

SELECT company, TRIM(COMPANY)
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET COMPANY = TRIM(COMPANY);

SELECT *
FROM layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry = 'crypto' 
where industry like 'crypto%';

SELECT distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
SET COUNTRY = trim(trailing '.' from country)
WHERE COUNTRY LIKE 'United States%';

SELECT `date`,
str_to_date(`date`, '%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
set `date` = str_to_date(`date`, '%m/%d/%Y');

SELECT `date`
FROM layoffs_staging2;

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` date;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

UPDATE layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null 
or industry = '';

select *
from layoffs_staging2
where company = 'bally%';

select *
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company =t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company =t2.company
set t1.industry = t2.industry 
where t1.industry is null 
and t2.industry is not null;

select *
from layoffs_staging2;

SELECT *
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete 
FROM layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

SELECT *
FROM layoffs_staging2;

alter table layoffs_staging2
drop column row_num;








