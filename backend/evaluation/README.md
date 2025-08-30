# HMDB Chatbot Evaluation System

## Quick Start

To run your chatbot evaluation, simply execute:

```bash
cd backend
python -m evaluation.run_evaluation
```

That's it! This script will automatically:
- ✅ Check your dependencies
- ✅ Validate your data files
- ✅ Test your evaluation questions
- ✅ Generate a complete report

## What You Need

1. **Virtual environment activated** (as usual)
2. **Data files in place** from Phase 1 and Phase 2
3. **Dependencies**: `pandas` and `numpy` (install with `pip install pandas numpy`)

## Directory Structure

```
evaluation/
├── run_evaluation.py          ← MAIN SCRIPT (run this!)
│
├── phase1/                     ← Data parsing results
│   ├── parsed_data/            ← Parsed metabolite & protein data
│   └── phase1_complete_parser.py
│
├── phase2/                     ← Question generation system
│   ├── generated_data/         ← Generated evaluation questions
│   └── [various library files]
│
└── approach2/                  ← Alternative data extraction
    └── [extraction tools]
```

## What the Evaluation Does

1. **Data Validation**: Checks if your parsed data is complete and correct
2. **Question Quality**: Validates your generated evaluation questions
3. **Answerability**: Measures how many questions have real answers
4. **Readiness Assessment**: Tells you if your evaluation is ready for use

## Output

The script generates:
- **Console report** with scores and recommendations
- **JSON report** saved as `evaluation_report_YYYYMMDD_HHMMSS.json`

## Scores Meaning

- **0.8+ = EXCELLENT**: Ready for production use
- **0.6-0.8 = GOOD**: Works but could be improved  
- **Below 0.6 = NEEDS WORK**: Check your data and question generation

## Troubleshooting

### "Missing data files"
- Run Phase 1 parsing first: `python phase1/phase1_complete_parser.py`
- Run Phase 2 generation: `python phase2/phase2_orchestrator.py`

### "Missing dependencies"
```bash
pip install pandas numpy
```

### "Low scores"
- Check your source data quality
- Review question generation parameters
- Ensure proper data parsing

## For Developers

If you need to modify the evaluation system:
- Core logic is in `phase2/` folder
- Question generation: `phase2/question_generator.py`
- Data sampling: `phase2/data_sampler.py`
- Quality assurance: `phase2/quality_assurance.py`

## Support

If you encounter issues:
1. Check the generated JSON report for details
2. Review console output for specific error messages
3. Ensure all data files are properly generated

---

**Remember**: This evaluation system tests the quality of your evaluation questions, not your chatbot directly. Once your evaluation is ready, you can use these questions to test your actual chatbot performance.
