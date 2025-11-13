SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Format the Data
-- 3. Work with Null values or Blank values
-- 4. Remove unnecessary Columns

-- Copy the original table to Edit/Work on

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *
FROM layoffs_staging;

-- Identify Duplicates

SELECT *,
ROW_NUMBER() OVER()
FROM layoffs_staging;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS raw_num
FROM layoffs_staging;

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS raw_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE raw_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Oda';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS raw_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE raw_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Casper';

WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS raw_num
FROM layoffs_staging
)
DELETE
FROM duplicate_cte
WHERE raw_num > 1;


CREATE TABLE `layoffs_staging3` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `raw_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging3;

INSERT INTO layoffs_staging3
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS raw_num
FROM layoffs_staging;

SELECT*
FROM layoffs_staging3
WHERE raw_num > 1;

DELETE
FROM layoffs_staging3
WHERE raw_num > 1;

-- Format Data

SELECT company, TRIM(company)
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET conpany = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_staging3
ORDER BY 1;
 
 SELECT *
 FROM layoffs_staging3
 WHERE industry like 'crypto%';
 
 UPDATE layoffs_staging3
 SET industry = 'Crypto'
 WHERE industry like 'crypto%';
 
 SELECT DISTINCT location
FROM layoffs_staging3
ORDER BY 1;
 
SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging3
ORDER BY 1; 

UPDATE layoffs_staging3
SET country = TRIM(TRAILING '.' FROM country)
;

SELECT DISTINCT country
FROM layoffs_staging3
ORDER BY 1;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

SELECT `date`, length(`date`)
FROM layoffs_staging3
ORDER BY length(`date`) DESC
;

SELECT `date`
FROM layoffs_staging3;

UPDATE layoffs_staging3
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

ALTER TABLE layoffs_staging3
MODIFY COLUMN `date` DATE;

-- 3. Work with Null values or Blank values

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3
WHERE company = 'Airbnb';

SELECT *
FROM layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
WHERE (t1.industry IS NULL OR t1.industry = '')
AND t2.industry IS NOT NULL ;   

UPDATE layoffs_staging3
SET industry = NULL
WHERE industry = '';

UPDATE layoffs_staging3 t1
JOIN layoffs_staging3 t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging3
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT *
FROM layoffs_staging3;

-- 4. Remove unnecessary Columns

ALTER TABLE layoffs_staging3
DROP COLUMN raw_num;

