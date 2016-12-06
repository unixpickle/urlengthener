(function() {

  function init() {
    var subButton = document.getElementById('submit');
    var urlBox = document.getElementById('url-box');
    var container = document.getElementById('post-pane');
    var shortened = false;
    subButton.addEventListener('click', function() {
      if (shortened) {
        shortened = false;
        urlBox.value = '';
        subButton.textContent = 'Shorten';
        return;
      }
      container.className = 'loading';
      shorten(function(err, newURL) {
        container.className = '';
        if (err) {
          window.alert('error: ' + err);
          return;
        }
        urlBox.value = newURL;
        shortened = true;
        subButton.textContent = 'Do Another';
      });
    });
  }

  function shorten(cb) {
    var url = document.getElementById('url-box').value;
    var delay = document.getElementById('delay').value;
    var duration = document.getElementById('expires').value;
    var queryURL = '/lengthen?url=' + encodeURIComponent(url) + '&delay=' +
      delay + '&duration=' + duration;
    var req = new XMLHttpRequest();
    req.onreadystatechange = function() {
      if (req.readyState === XMLHttpRequest.DONE) {
        if (req.status == 200) {
          cb(null, location.origin+'/lengthened/'+req.responseText);
        } else {
          cb('status code: ' + req.status);
        }
      } else if (!req.readyState) {
        cb('network error');
      }
    };
    req.open('GET', queryURL);
    req.send(null);
  }

  window.addEventListener('load', init);

})();
