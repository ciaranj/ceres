exports.aggregateValues= function(method, values) {
  // The points will always be non-null.
  if( method == 'avg' || method == 'average' ) {
    
    var sum= 0;
    if( values.length > 0 ) {
      for(var k in values) { sum += values[k]; }
      sum= sum / values.length;
    }
    return sum;
  }
  else if( method == 'sum' ) {
    var sum= 0;
    for(var k in values) { sum += values[k]; }
    return sum
  }
  else if( method == 'min' ) {
    return Math.min.apply(this, values);
  }
  else if( method == 'max' ) {
    return Math.max.apply(this, values);
  }
  else if( method == 'median' ) {
    values.sort();
    return values[ values.length / 2 ];
  }
}