SELECT * FROM layoffs;

-- 1. DATABASE & STAGING SETUP
CREATE DATABASE IF NOT EXISTS world_layoffs;
USE world_layoffs;

-- Create a staging table to avoid working on raw data
CREATE TABLE layoffs_staging LIKE layoffs;
INSERT INTO layoffs_staging SELECT * FROM layoffs;


-- 2. DEDUPLICATION (Removing Duplicates)
-- MySQL doesn't allow deleting from a CTE, so we create a second staging table with a row_num column
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
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
    PARTITION BY company, location, industry, total_laid_off, 
    percentage_laid_off, `date`, stage, country, funds_raised_millions
) AS row_num
FROM layoffs_staging;

-- Delete rows where row_num is greater than 1
SET SQL_SAFE_UPDATES = 0;
DELETE FROM layoffs_staging2 WHERE row_num > 1;

-- 3. STANDARDIZATION
-- Trim whitespace from company names
UPDATE layoffs_staging2 SET company = TRIM(company);

-- Consolidate industry names (e.g., merging "Crypto Currency" and "Crypto" into "Crypto")
UPDATE layoffs_staging2 SET industry = 'Crypto' WHERE industry LIKE 'Crypto%';

-- Fix trailing punctuation in country names (e.g., "United States." to "United States")
UPDATE layoffs_staging2 SET country = TRIM(TRAILING '.' FROM country) WHERE country LIKE 'United States%';

-- Convert 'date' column from string/text to a proper SQL DATE format
UPDATE layoffs_staging2 SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');
ALTER TABLE layoffs_staging2 MODIFY COLUMN `date` DATE;

-- 4. HANDLING NULLS & BLANKS
-- Standardize blanks to NULLs for industry
UPDATE layoffs_staging2 SET industry = NULL WHERE industry = '';

-- Populate NULL industries by matching data from other rows of the same company
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

-- 5. FINAL CLEANUP
-- Remove rows that are missing both total layoffs and percentage (unusable for analysis)
DELETE FROM layoffs_staging2 
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

-- Remove the helper column used for deduplication
ALTER TABLE layoffs_staging2 DROP COLUMN row_num;

-- View the final cleaned data
SELECT * FROM layoffs_staging2;