exports.aggregateValues= function(method, values) {
  function getSum() {
    var sum= 0;
    if( values.length > 0 ) {
      for(var k=0; k<values.length; k++) { sum += values[k]; }
    }
    return sum;
  }
  // The points will always be non-null.
  if( method == 'avg' || method == 'average' ) {
    if( values.length > 0 ) {
      return getSum() / values.length;
    }
    else return 0;
  }
  else if( method == 'sum' ) {
    return getSum();
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