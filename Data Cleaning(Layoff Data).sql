#Layoffs data cleaning

# Things to do
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary


SELECT * 
FROM world_layoffs.layoffs;

-- Create duplicate table for cleaning data
CREATE TABLE world_layoffs.layoffs_staging 
LIKE world_layoffs.layoffs;

INSERT layoffs_staging 
SELECT * FROM world_layoffs.layoffs;

SELECT * 
FROM layoffs_staging;

# 1. Remove Duplicates

-- Check Duplicates
SELECT *
FROM world_layoffs.layoffs_staging;

SELECT company, industry, total_laid_off,`date`,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,`date`) AS row_num
FROM world_layoffs.layoffs_staging;

-- Check sample item duplicate
select*
from layoffs_staging
WHERE company ='Casper';

-- Create CTE and do partition to check duplicate items
with duplicate_cte AS(
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging
)
select *
from duplicate_cte
where row_num>1 ;

-- Create new table to add new column row_num to find out duplicate items
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

select*from
layoffs_staging2
WHERE row_num >1;

# Insert all atrbute to new stage table
INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER (
PARTITION BY company,location,industry, total_laid_off, percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM world_layoffs.layoffs_staging;

SET SQL_SAFE_UPDATES = 0;

select*from
layoffs_staging2;

-- After we do insert to new table, we can remove the duplicate item 
delete
from layoffs_staging2
WHERE row_num >1;

select*from
layoffs_staging2;

# 2. Standarize 

-- Delete Blank Space
select company, trim(company)
from layoffs_staging2;

 
update layoffs_staging2
set company=trim(company);

select distinct industry
from layoffs_staging2
order by 1;

-- Deleting and updating industry names that have different writing but are the same industry
select* 
from layoffs_staging2
where industry LIKE 'Crypto%';

UPDATE layoffs_staging2
set industry= 'Crypto'
WHERE industry like 'Crypto%';

select distinct country
from layoffs_staging2
order by 1;

-- Deleting and updating country names that have different writing but are the same country
select distinct country, TRIM(TRAILING '.' FROM country)
from layoffs_staging2
order by 1;

UPDATE layoffs_staging2
set country= TRIM(TRAILING '.' FROM country)
where country like 'United States%';

select distinct country
from layoffs_staging2
order by 1;

-- Change Date Format 
select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date`= str_to_date(`date`, '%m/%d/%Y');

-- Change Date data type 
alter table layoffs_staging2
modify column `date` DATE;

-- Execute the NULL(Delete or populated by similar data) 
select *
from layoffs_staging2 where total_laid_off IS NULL
and percentage_laid_off IS NULL;

select *
from layoffs_staging2
where industry is NULL 
OR industry= '';

select *
from layoffs_staging2
where company = 'Airbnb';

select *
from layoffs_staging2
where company Like 'Bally%';

update layoffs_staging2
set industry = NULL
where industry= '';

select * 
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company= t2.company
    AND t1.location=t1.location
WHERE (t1.industry IS NULL OR t1.industry= '')
AND t2.industry IS NOT NULL;


update layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company= t2.company
SET t1.industry= t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL; 


# 4. Remove any columns and row

-- Delete data that has many empty attributes, because it cannot be filled with the same data due to lack of information, so it will be deleted
delete
from layoffs_staging2 where total_laid_off IS NULL
and percentage_laid_off IS NULL;

select * from
layoffs_staging2;

alter table layoffs_staging2
drop column row_num;




    
