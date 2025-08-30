#!/usr/bin/env python3
"""
HMDB Chatbot Evaluation System
==============================

This is the MAIN script to run your chatbot evaluation.

Simply run this script to:
1. Validate your evaluation data
2. Test your evaluation questions
3. Get a complete readiness report

Usage:
    cd backend
    python -m evaluation.run_evaluation

Requirements:
- Virtual environment activated
- All data files in place (phase1/parsed_data/ and phase2/generated_data/)
"""

import sys
import os
import json
import logging
from pathlib import Path
from datetime import datetime

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger(__name__)

def check_dependencies():
    """Check if required dependencies are available"""
    try:
        import pandas
        import numpy
        return True
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Install with: pip install pandas numpy")
        return False

def check_data_files():
    """Check if required data files exist"""
    eval_dir = Path(__file__).parent
    
    required_files = [
        "phase1/parsed_data/metabolites_complete_final.json",
        "phase1/parsed_data/proteins_complete_final_CORRECTED.json",
        "phase2/generated_data/phase2_final_question_set.json"
    ]
    
    missing_files = []
    for file_path in required_files:
        full_path = eval_dir / file_path
        if not full_path.exists():
            missing_files.append(file_path)
    
    if missing_files:
        print("❌ Missing required data files:")
        for file_path in missing_files:
            print(f"   - {file_path}")
        return False
    
    return True

def validate_questions():
    """Validate the generated questions"""
    eval_dir = Path(__file__).parent
    questions_file = eval_dir / "phase2/generated_data/phase2_final_question_set.json"
    
    try:
        with open(questions_file, 'r') as f:
            data = json.load(f)
            questions = data.get('questions', [])
        
        if not questions:
            return False, "No questions found in question set"
        
        # Basic validation
        valid_questions = 0
        total_questions = len(questions)
        answered_questions = 0
        
        for q in questions:
            if all(field in q for field in ['question', 'answer', 'entity_id', 'category']):
                valid_questions += 1
                if q.get('answer') is not None:
                    answered_questions += 1
        
        validation_score = valid_questions / total_questions
        answerability_rate = answered_questions / total_questions
        
        return True, {
            'total_questions': total_questions,
            'valid_questions': valid_questions,
            'answered_questions': answered_questions,
            'validation_score': validation_score,
            'answerability_rate': answerability_rate
        }
        
    except Exception as e:
        return False, f"Error validating questions: {e}"

def generate_report(validation_results):
    """Generate evaluation report"""
    eval_dir = Path(__file__).parent
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = eval_dir / f"evaluation_report_{timestamp}.json"
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'validation_results': validation_results,
        'status': 'completed'
    }
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    return report_file

def main():
    """Main evaluation function"""
    print("\n" + "="*60)
    print("🧪 HMDB CHATBOT EVALUATION SYSTEM")
    print("="*60)
    
    # Step 1: Check dependencies
    print("\n📋 STEP 1: Checking Dependencies")
    print("-" * 30)
    if not check_dependencies():
        print("❌ Dependency check failed")
        return 1
    print("✅ Dependencies OK")
    
    # Step 2: Check data files
    print("\n📂 STEP 2: Checking Data Files")
    print("-" * 30)
    if not check_data_files():
        print("❌ Data file check failed")
        print("\nMake sure you have run:")
        print("1. Phase 1 parsing (phase1_complete_parser.py)")
        print("2. Phase 2 question generation (phase2_orchestrator.py)")
        return 1
    print("✅ Data files OK")
    
    # Step 3: Validate questions
    print("\n❓ STEP 3: Validating Questions")
    print("-" * 30)
    success, results = validate_questions()
    if not success:
        print(f"❌ Question validation failed: {results}")
        return 1
    
    print(f"✅ Question validation passed")
    print(f"   📊 Total Questions: {results['total_questions']}")
    print(f"   ✅ Valid Questions: {results['valid_questions']}")
    print(f"   💬 Answered Questions: {results['answered_questions']}")
    print(f"   📈 Validation Score: {results['validation_score']:.2f}")
    print(f"   🎯 Answerability Rate: {results['answerability_rate']:.2f}")
    
    # Step 4: Generate report
    print("\n📊 STEP 4: Generating Report")
    print("-" * 30)
    report_file = generate_report(results)
    print(f"✅ Report saved to: {report_file}")
    
    # Final assessment
    print("\n" + "="*60)
    print("📊 EVALUATION SUMMARY")
    print("="*60)
    
    validation_score = results['validation_score']
    answerability_rate = results['answerability_rate']
    
    overall_score = (validation_score + answerability_rate) / 2
    
    print(f"\n🎯 Overall Score: {overall_score:.2f}/1.00")
    
    if overall_score >= 0.8:
        print("🎉 EXCELLENT: Your evaluation system is ready for use!")
        status = "READY"
    elif overall_score >= 0.6:
        print("⚠️  GOOD: Your evaluation system works but could be improved")
        status = "NEEDS_IMPROVEMENT"
    else:
        print("❌ ISSUES: Your evaluation system needs attention")
        status = "NEEDS_FIXES"
    
    print(f"\n📋 Next Steps:")
    if status == "READY":
        print("   ✅ System is ready for production use")
        print("   📝 You can use this evaluation data to test your chatbot")
    elif status == "NEEDS_IMPROVEMENT":
        print("   🔧 Consider regenerating questions with better data")
        print("   📊 Review question quality and answerability")
    else:
        print("   🚨 Check your data parsing and question generation")
        print("   🔍 Review logs for specific issues")
    
    print("\n" + "="*60)
    
    return 0 if status == "READY" else 1

if __name__ == "__main__":
    sys.exit(main())