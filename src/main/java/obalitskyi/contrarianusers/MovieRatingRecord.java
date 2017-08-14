package obalitskyi.contrarianusers;

import java.io.Serializable;



public class MovieRatingRecord implements Serializable {
    private Integer customerId;
    private Integer rating;
    private String dateOfRating;

    public MovieRatingRecord(Integer customerId, Integer rating, String dateOfRating) {
        this.customerId = customerId;
        this.rating = rating;
        this.dateOfRating = dateOfRating;
    }

    public MovieRatingRecord() {
    }

    public static MovieRatingRecord createDummy () {
        return new MovieRatingRecord(-1, -1, "");
    }

    public Integer getCustomerId() {
        return customerId;
    }

    public void setCustomerId(Integer customerId) {
        this.customerId = customerId;
    }

    public Integer getRating() {
        return rating;
    }

    public void setRating(Integer rating) {
        this.rating = rating;
    }

    public String getDateOfRating() {
        return dateOfRating;
    }

    public void setDateOfRating(String dateOfRating) {
        this.dateOfRating = dateOfRating;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MovieRatingRecord that = (MovieRatingRecord) o;

        if (customerId != null ? !customerId.equals(that.customerId) : that.customerId != null) return false;
        if (rating != null ? !rating.equals(that.rating) : that.rating != null) return false;
        return dateOfRating != null ? dateOfRating.equals(that.dateOfRating) : that.dateOfRating == null;
    }

    @Override
    public int hashCode() {
        int result = customerId != null ? customerId.hashCode() : 0;
        result = 31 * result + (rating != null ? rating.hashCode() : 0);
        result = 31 * result + (dateOfRating != null ? dateOfRating.hashCode() : 0);
        return result;
    }
}

