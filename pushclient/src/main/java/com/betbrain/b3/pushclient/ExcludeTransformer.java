package com.betbrain.b3.pushclient;

import flexjson.transformer.AbstractTransformer;

public class ExcludeTransformer extends AbstractTransformer {

  @Override
  public Boolean isInline() {
      return true;
  }

  public void transform(Object object) {
      // Do nothing, null objects are not serialized.
      return;
  }
}