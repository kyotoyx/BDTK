/*
 * Copyright (c) 2022 Intel Corporation.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "exec/nextgen/jitlib/llvmjit/LLVMJITValue.h"

#include <llvm/IR/Value.h>

#include "exec/nextgen/jitlib/base/ValueTypes.h"
#include "util/Logger.h"

namespace cider::jitlib {

JITValue& LLVMJITValue::assign(JITValue& value) {
  if (!is_variable_) {
    LOG(FATAL) << "JITValue " << getValueName()
               << "is not a variable in LLVMJITValue::assign.";
  }
  store(static_cast<LLVMJITValue&>(value));
  return *this;
}

JITValuePointer LLVMJITValue::andOp(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL:
      ans = getFunctionBuilder(parent_function_).CreateAnd(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for and operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "and", false);
}

JITValuePointer LLVMJITValue::orOp(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL:
      ans = getFunctionBuilder(parent_function_).CreateOr(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for or operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "or", false);
}

JITValuePointer LLVMJITValue::notOp() {
  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::BOOL:
      ans = getFunctionBuilder(parent_function_).CreateNot(load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for not operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "not", false);
}

JITValuePointer LLVMJITValue::mod(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "mod");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateSRem(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFRem(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for mod operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "mod", false);
}

JITValuePointer LLVMJITValue::div(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "div");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateSDiv(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFDiv(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for div operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "div", false);
}

JITValuePointer LLVMJITValue::mul(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "mul");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateMul(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFMul(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for mul operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "mul", false);
}

JITValuePointer LLVMJITValue::sub(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "sub");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateSub(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFSub(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for sub operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "sub", false);
}

JITValuePointer LLVMJITValue::add(JITValue& rh) {
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), "add");

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_).CreateAdd(load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_).CreateFAdd(load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for add operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, "add", false);
}

JITValuePointer LLVMJITValue::createCmpInstruction(llvm::CmpInst::Predicate ICmpType,
                                                   llvm::CmpInst::Predicate FCmpType,
                                                   JITValue& rh,
                                                   const char* value) {
  checkOprandsType(this->getValueTypeTag(), rh.getValueTypeTag(), value);
  LLVMJITValue& llvm_rh = static_cast<LLVMJITValue&>(rh);

  llvm::Value* ans = nullptr;
  switch (getValueTypeTag()) {
    case JITTypeTag::INT8:
    case JITTypeTag::INT16:
    case JITTypeTag::INT32:
    case JITTypeTag::INT64:
      ans = getFunctionBuilder(parent_function_)
                .CreateICmp(ICmpType, load(), llvm_rh.load());
      break;
    case JITTypeTag::FLOAT:
    case JITTypeTag::DOUBLE:
      ans = getFunctionBuilder(parent_function_)
                .CreateFCmp(FCmpType, load(), llvm_rh.load());
      break;
    default:
      LOG(FATAL) << "Invalid JITValue type for compare operation. Name=" << getValueName()
                 << ", Type=" << getJITTypeName(getValueTypeTag()) << ".";
  }

  return std::make_unique<LLVMJITValue>(
      getValueTypeTag(), parent_function_, ans, value, false);
}

JITValuePointer LLVMJITValue::eq(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_EQ, llvm::CmpInst::FCMP_OEQ, rh, "eq");
}

JITValuePointer LLVMJITValue::ne(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_NE, llvm::CmpInst::FCMP_ONE, rh, "ne");
}

JITValuePointer LLVMJITValue::lt(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SLT, llvm::CmpInst::FCMP_OLT, rh, "lt");
}

JITValuePointer LLVMJITValue::le(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SLE, llvm::CmpInst::FCMP_OLE, rh, "le");
}

JITValuePointer LLVMJITValue::gt(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SGT, llvm::CmpInst::FCMP_OGT, rh, "gt");
}

JITValuePointer LLVMJITValue::ge(JITValue& rh) {
  return createCmpInstruction(llvm::CmpInst::ICMP_SGE, llvm::CmpInst::FCMP_OGE, rh, "ge");
}

void LLVMJITValue::checkOprandsType(JITTypeTag lh, JITTypeTag rh, const char* op) {
  if (lh != rh) {
    LOG(FATAL) << "Oprands type doesn't match in LLVMJITValue operator " << op
               << " lh=" << getJITTypeName(lh) << ", rh=" << getJITTypeName(rh) << ".";
  }
}

llvm::Value* LLVMJITValue::load() {
  if (is_variable_) {
    return getFunctionBuilder(parent_function_).CreateLoad(llvm_value_, false);
  } else {
    return llvm_value_;
  }
}

llvm::Value* LLVMJITValue::store(LLVMJITValue& rh) {
  if (is_variable_) {
    return getFunctionBuilder(parent_function_)
        .CreateStore(rh.load(), llvm_value_, false);
  }
  return nullptr;
}

};  // namespace cider::jitlib