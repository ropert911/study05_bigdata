package com.xq.study.demo_milib

/**
  * @author sk-qianxiao
  * @date 2019/11/12
  */
class MovieDis extends Serializable {
  var id: Int = 0;
  var title: String = null;
  var distance: Double = 0;
  var movieType: String = null;

  def this(_id: Int, _title: String, _distance: Double, _movieType: String) {
    this();
    this.id = _id;
    this.title = _title;
    this.distance = _distance;
    this.movieType = _movieType;
  }

  override def toString(): String = {
    return "MovieDis [id=" + id + ", title=" + title + ", distance=" + distance + ", movieType=" + movieType + "]";
  }
}
